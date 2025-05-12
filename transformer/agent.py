from datetime import datetime

import pandas as pd

from config import settings
from transformer.const import COLUMNS, HIST_COLUMNS


class Agent:

    def __init__(self, data):
        self.data = data

    def transform(self) -> dict:
        return {
            settings.SUMMARY_OUTPUT_TABLE: self.transform_summary(),
            settings.SUMMARY_HIST_OUTPUT_TABLE: self.transform_summary_history(),
        }

    def transform_summary(self) -> pd.DataFrame:
        rows = []
        for ticker, fundamentals in self.data.items():
            rows.append(self._parse_single_ticker_fundamentals(fundamentals, ticker))
        return pd.DataFrame(rows, columns=COLUMNS)

    def transform_summary_history(self) -> pd.DataFrame:
        rows = []
        for ticker, fundamentals in self.data.items():
            summary_struct = self._fill_summaries_object(fundamentals)
            rows.extend(self._build_multi_rows(ticker, summary_struct))
        return pd.DataFrame(rows, columns=HIST_COLUMNS)

    def _build_multi_rows(self, ticker: str, summary: dict) -> list:
        """
        Produces multiple rows per ticker: up to 6 quarterlies and 2 yearlies
        for each of Balance Sheet, Cash Flow, and Income Statement.
        """
        rows = []
        ts_now = datetime.utcnow()
        updated_at = self._safe_val(summary["General"].get("UpdatedAt"))
        currency = self._safe_val(summary["General"].get("CurrencyCode"))

        # Collect all statement lists
        bs_q = sorted(
            summary["Financials"]["Balance_Sheet"]["quarterly"],
            key=lambda x: x.get("date", ""),
            reverse=True,
        )[:6]
        cf_q = sorted(
            summary["Financials"]["Cash_Flow"]["quarterly"],
            key=lambda x: x.get("date", ""),
            reverse=True,
        )[:6]
        is_q = sorted(
            summary["Financials"]["Income_Statement"]["quarterly"],
            key=lambda x: x.get("date", ""),
            reverse=True,
        )[:6]

        bs_y = sorted(
            summary["Financials"]["Balance_Sheet"]["yearly"],
            key=lambda x: x.get("date", ""),
            reverse=True,
        )[:2]
        cf_y = sorted(
            summary["Financials"]["Cash_Flow"]["yearly"],
            key=lambda x: x.get("date", ""),
            reverse=True,
        )[:2]
        is_y = sorted(
            summary["Financials"]["Income_Statement"]["yearly"],
            key=lambda x: x.get("date", ""),
            reverse=True,
        )[:2]

        # Quarterlies
        max_q = max(len(bs_q), len(cf_q), len(is_q))
        for i in range(max_q):
            rows.append(
                self._build_one_row(
                    eodhd_ticker=ticker,
                    timestamp_created_utc=ts_now,
                    updated_at=updated_at,
                    period="quarterly",
                    currency_code=currency,
                    bs_info=bs_q[i] if i < len(bs_q) else None,
                    cf_info=cf_q[i] if i < len(cf_q) else None,
                    is_info=is_q[i] if i < len(is_q) else None,
                )
            )

        # Yearlies
        max_y = max(len(bs_y), len(cf_y), len(is_y))
        for i in range(max_y):
            rows.append(
                self._build_one_row(
                    eodhd_ticker=ticker,
                    timestamp_created_utc=ts_now,
                    updated_at=updated_at,
                    period="yearly",
                    currency_code=currency,
                    bs_info=bs_y[i] if i < len(bs_y) else None,
                    cf_info=cf_y[i] if i < len(cf_y) else None,
                    is_info=is_y[i] if i < len(is_y) else None,
                )
            )

        return rows

    def _build_one_row(
        self,
        eodhd_ticker: str,
        timestamp_created_utc: datetime,
        updated_at,
        period: str,
        currency_code,
        bs_info: dict,
        cf_info: dict,
        is_info: dict,
    ) -> dict:
        """
        Constructs one historical row from
        - a single BalanceSheetInfo dict (bs_info)
        - a single CashFlowInfo dict (cf_info)
        - a single IncomeStatementInfo dict (is_info)
        plus minimal fields from "General" (updated_at, currency_code)
        and the 'period' label.
        """
        bs_info = bs_info or {}
        cf_info = cf_info or {}
        is_info = is_info or {}

        row = {
            "eodhd_ticker": eodhd_ticker,
            "timestamp_created_utc": timestamp_created_utc,
            "updated_at": updated_at,
            "Period": period,
            "CurrencyCode": currency_code,
        }
        self._extract_balance_sheet_fields(row, bs_info, prefix="balance_sheet_")
        self._extract_cash_flow_fields(row, cf_info, prefix="cash_")
        self._extract_income_statement_fields(row, is_info, prefix="income_")
        return row

    def _parse_single_ticker_fundamentals(
        self, fundamentals: dict, ticker: str
    ) -> dict:
        """
        Builds a single-row dictionary (the summary),
        pulling the 'latest' from each sub-section.
        """
        row = {"timestamp_created_utc": datetime.utcnow(), "eodhd_ticker": ticker}
        if not fundamentals:
            return row  # all other fields remain missing/None

        # Extract top-level sections
        general = fundamentals.get("General", {})
        highlights = fundamentals.get("Highlights", {})
        valuation = fundamentals.get("Valuation", {})
        splits_dividends = fundamentals.get("SplitsDividends", {})
        shares_stats = fundamentals.get("SharesStats", {})
        analyst_ratings = fundamentals.get("AnalystRatings", {})
        financials = fundamentals.get("Financials", {})

        # Fill row from each section
        self._extract_general_fields(row, general)
        self._extract_highlights_fields(row, highlights)
        self._extract_valuation_fields(row, valuation)
        self._extract_splits_dividends_fields(row, splits_dividends)
        self._extract_shares_stats_fields(row, shares_stats)
        self._extract_analyst_ratings_fields(row, analyst_ratings)

        # Grab 'latest' from each Financials sub-section
        bs = self._get_latest_financials(financials.get("Balance_Sheet", {}))
        cf = self._get_latest_financials(financials.get("Cash_Flow", {}))
        inc = self._get_latest_financials(financials.get("Income_Statement", {}))

        self._extract_balance_sheet_fields(row, bs, prefix="balance_sheet_")
        self._extract_cash_flow_fields(row, cf, prefix="cash_")
        self._extract_income_statement_fields(row, inc, prefix="income_")

        return row

    def _fill_summaries_object(self, fundamentals_data: dict) -> dict:
        """
        Gathers *all* quarterlies and yearlies for BS/CF/IS,
        plus minimal 'General' fields (UpdatedAt, CurrencyCode).
        """
        root = fundamentals_data or {}
        general = root.get("General", {})
        general_dict = {
            "UpdatedAt": general.get("UpdatedAt"),
            "CurrencyCode": general.get("CurrencyCode"),
        }

        fin_dict = {
            "Balance_Sheet": {"quarterly": [], "yearly": []},
            "Cash_Flow": {"quarterly": [], "yearly": []},
            "Income_Statement": {"quarterly": [], "yearly": []},
        }
        f = root.get("Financials", {})

        self._collect_quarterly_yearly(
            f.get("Balance_Sheet", {}), fin_dict["Balance_Sheet"]
        )
        self._collect_quarterly_yearly(f.get("Cash_Flow", {}), fin_dict["Cash_Flow"])
        self._collect_quarterly_yearly(
            f.get("Income_Statement", {}), fin_dict["Income_Statement"]
        )
        return {"General": general_dict, "Financials": fin_dict}

    def _collect_quarterly_yearly(self, section: dict, target: dict):
        """Helper to add all quarterly/yearly items from `section` into `target`."""
        for period_key in ("quarterly", "yearly"):
            data_map = section.get(period_key, {})
            for date_key, val in data_map.items():
                if isinstance(val, dict):
                    # ensure "date" if missing
                    item = dict(val)
                    if "date" not in item:
                        item["date"] = date_key
                    target[period_key].append(item)

    def _extract_general_fields(self, row: dict, data: dict):
        row["updated_at"] = self._safe_val(data.get("UpdatedAt"))
        row["CurrencyCode"] = self._safe_val(data.get("CurrencyCode"))
        row["Sector"] = self._safe_val(data.get("Sector"))
        row["Industry"] = self._safe_val(data.get("Industry"))
        row["GicSector"] = self._safe_val(data.get("GicSector"))
        row["GicGroup"] = self._safe_val(data.get("GicGroup"))
        row["GicIndustry"] = self._safe_val(data.get("GicIndustry"))
        row["GicSubIndustry"] = self._safe_val(data.get("GicSubIndustry"))

        addr = data.get("AddressData", {}) or {}
        row["Street"] = self._safe_val(addr.get("Street"))
        row["City"] = self._safe_val(addr.get("City"))
        row["State"] = self._safe_val(addr.get("State"))
        row["Country"] = self._safe_val(addr.get("Country"))
        row["ZIP"] = self._safe_val(addr.get("ZIP"))

    def _extract_highlights_fields(self, row: dict, data: dict):
        row["MarketCapitalizationMln"] = self._safe_val(
            data.get("MarketCapitalizationMln")
        )
        row["PERatio"] = self._safe_val(data.get("PERatio"))
        row["PEGRatio"] = self._safe_val(data.get("PEGRatio"))
        row["WallStreetTargetPrice"] = self._safe_val(data.get("WallStreetTargetPrice"))
        row["BookValue"] = self._safe_val(data.get("BookValue"))
        row["DividendYield"] = self._safe_val(data.get("DividendYield"))
        row["ProfitMargin"] = self._safe_val(data.get("ProfitMargin"))
        row["OperatingMarginTTM"] = self._safe_val(data.get("OperatingMarginTTM"))
        row["ReturnOnAssetsTTM"] = self._safe_val(data.get("ReturnOnAssetsTTM"))
        row["ReturnOnEquityTTM"] = self._safe_val(data.get("ReturnOnEquityTTM"))
        row["RevenueTTM"] = self._safe_val(data.get("RevenueTTM"))
        row["RevenuePerShareTTM"] = self._safe_val(data.get("RevenuePerShareTTM"))

    def _extract_valuation_fields(self, row: dict, data: dict):
        row["TrailingPE"] = self._safe_val(data.get("TrailingPE"))
        row["ForwardPE"] = self._safe_val(data.get("ForwardPE"))
        row["PriceSalesTTM"] = self._safe_val(data.get("PriceSalesTTM"))
        row["PriceBookMRQ"] = self._safe_val(data.get("PriceBookMRQ"))
        row["EnterpriseValue"] = self._safe_val(data.get("EnterpriseValue"))
        row["EnterpriseValueRevenue"] = self._safe_val(
            data.get("EnterpriseValueRevenue")
        )
        row["EnterpriseValueEbitda"] = self._safe_val(data.get("EnterpriseValueEbitda"))

    def _extract_splits_dividends_fields(self, row: dict, data: dict):
        row["ForwardAnnualDividendRate"] = self._safe_val(
            data.get("ForwardAnnualDividendRate")
        )
        row["ForwardAnnualDividendYield"] = self._safe_val(
            data.get("ForwardAnnualDividendYield")
        )
        row["PayoutRatio"] = self._safe_val(data.get("PayoutRatio"))

    def _extract_shares_stats_fields(self, row: dict, data: dict):
        row["SharesOutstanding"] = self._safe_val(data.get("SharesOutstanding"))

    def _extract_analyst_ratings_fields(self, row: dict, data: dict):
        row["Rating"] = self._safe_val(data.get("Rating"))

    def _extract_balance_sheet_fields(self, row: dict, data: dict, prefix=""):
        """
        Fills row[...] with all the known BalanceSheet fields,
        optionally applying a prefix like 'balance_sheet_'.
        """
        row[prefix + "date"] = self._safe_val(data.get("date"))
        row[prefix + "filing_date"] = self._safe_val(data.get("filing_date"))
        row["totalAssets"] = self._safe_val(data.get("totalAssets"))
        row["intangibleAssets"] = self._safe_val(data.get("intangibleAssets"))
        row["earningAssets"] = self._safe_val(data.get("earningAssets"))
        row["otherCurrentAssets"] = self._safe_val(data.get("otherCurrentAssets"))
        row["totalLiab"] = self._safe_val(data.get("totalLiab"))
        row["totalStockholderEquity"] = self._safe_val(
            data.get("totalStockholderEquity")
        )
        row["deferredLongTermLiab"] = self._safe_val(data.get("deferredLongTermLiab"))
        row["otherCurrentLiab"] = self._safe_val(data.get("otherCurrentLiab"))
        row["commonStock"] = self._safe_val(data.get("commonStock"))
        row["retainedEarnings"] = self._safe_val(data.get("retainedEarnings"))
        row["otherLiab"] = self._safe_val(data.get("otherLiab"))
        row["goodWill"] = self._safe_val(data.get("goodWill"))
        row["otherAssets"] = self._safe_val(data.get("otherAssets"))
        row["cash"] = self._safe_val(data.get("cash"))
        row["totalCurrentLiabilities"] = self._safe_val(
            data.get("totalCurrentLiabilities")
        )
        row["netDebt"] = self._safe_val(data.get("netDebt"))
        row["shortTermDebt"] = self._safe_val(data.get("shortTermDebt"))
        row["shortLongTermDebt"] = self._safe_val(data.get("shortLongTermDebt"))
        row["shortLongTermDebtTotal"] = self._safe_val(
            data.get("shortLongTermDebtTotal")
        )
        row["otherStockholderEquity"] = self._safe_val(
            data.get("otherStockholderEquity")
        )
        row["propertyPlantEquipment"] = self._safe_val(
            data.get("propertyPlantEquipment")
        )
        row["totalCurrentAssets"] = self._safe_val(data.get("totalCurrentAssets"))
        row["longTermInvestments"] = self._safe_val(data.get("longTermInvestments"))
        row["netTangibleAssets"] = self._safe_val(data.get("netTangibleAssets"))
        row["shortTermInvestments"] = self._safe_val(data.get("shortTermInvestments"))
        row["netReceivables"] = self._safe_val(data.get("netReceivables"))
        row["longTermDebt"] = self._safe_val(data.get("longTermDebt"))
        row["inventory"] = self._safe_val(data.get("inventory"))
        row["accountsPayable"] = self._safe_val(data.get("accountsPayable"))
        row["totalPermanentEquity"] = self._safe_val(data.get("totalPermanentEquity"))
        row["noncontrollingInterestInConsolidatedEntity"] = self._safe_val(
            data.get("noncontrollingInterestInConsolidatedEntity")
        )
        row["temporaryEquityRedeemableNoncontrollingInterests"] = self._safe_val(
            data.get("temporaryEquityRedeemableNoncontrollingInterests")
        )
        row["accumulatedOtherComprehensiveIncome"] = self._safe_val(
            data.get("accumulatedOtherComprehensiveIncome")
        )
        row["additionalPaidInCapital"] = self._safe_val(
            data.get("additionalPaidInCapital")
        )
        row["commonStockTotalEquity"] = self._safe_val(
            data.get("commonStockTotalEquity")
        )
        row["preferredStockTotalEquity"] = self._safe_val(
            data.get("preferredStockTotalEquity")
        )
        row["retainedEarningsTotalEquity"] = self._safe_val(
            data.get("retainedEarningsTotalEquity")
        )
        row["treasuryStock"] = self._safe_val(data.get("treasuryStock"))
        row["accumulatedAmortization"] = self._safe_val(
            data.get("accumulatedAmortization")
        )
        row["nonCurrrentAssetsOther"] = self._safe_val(
            data.get("nonCurrrentAssetsOther")
        )
        row["deferredLongTermAssetCharges"] = self._safe_val(
            data.get("deferredLongTermAssetCharges")
        )
        row["nonCurrentAssetsTotal"] = self._safe_val(data.get("nonCurrentAssetsTotal"))
        row["capitalLeaseObligations"] = self._safe_val(
            data.get("capitalLeaseObligations")
        )
        row["longTermDebtTotal"] = self._safe_val(data.get("longTermDebtTotal"))
        row["nonCurrentLiabilitiesOther"] = self._safe_val(
            data.get("nonCurrentLiabilitiesOther")
        )
        row["nonCurrentLiabilitiesTotal"] = self._safe_val(
            data.get("nonCurrentLiabilitiesTotal")
        )
        row["negativeGoodwill"] = self._safe_val(data.get("negativeGoodwill"))
        row["warrants"] = self._safe_val(data.get("warrants"))
        row["preferredStockRedeemable"] = self._safe_val(
            data.get("preferredStockRedeemable")
        )
        row["capitalSurpluse"] = self._safe_val(data.get("capitalSurpluse"))
        row["liabilitiesAndStockholdersEquity"] = self._safe_val(
            data.get("liabilitiesAndStockholdersEquity")
        )
        row["cashAndShortTermInvestments"] = self._safe_val(
            data.get("cashAndShortTermInvestments")
        )
        row["propertyPlantAndEquipmentGross"] = self._safe_val(
            data.get("propertyPlantAndEquipmentGross")
        )
        row["propertyPlantAndEquipmentNet"] = self._safe_val(
            data.get("propertyPlantAndEquipmentNet")
        )
        row["accumulatedDepreciation"] = self._safe_val(
            data.get("accumulatedDepreciation")
        )
        row["netWorkingCapital"] = self._safe_val(data.get("netWorkingCapital"))
        row["netInvestedCapital"] = self._safe_val(data.get("netInvestedCapital"))
        row["commonStockSharesOutstanding"] = self._safe_val(
            data.get("commonStockSharesOutstanding")
        )

    def _extract_cash_flow_fields(self, row: dict, data: dict, prefix=""):
        row[prefix + "date"] = self._safe_val(data.get("date"))
        row[prefix + "filing_date"] = self._safe_val(data.get("filing_date"))
        row["investments"] = self._safe_val(data.get("investments"))
        row["changeToLiabilities"] = self._safe_val(data.get("changeToLiabilities"))
        row["totalCashflowsFromInvestingActivities"] = self._safe_val(
            data.get("totalCashflowsFromInvestingActivities")
        )
        row["netBorrowings"] = self._safe_val(data.get("netBorrowings"))
        row["totalCashFromFinancingActivities"] = self._safe_val(
            data.get("totalCashFromFinancingActivities")
        )
        row["changeToOperatingActivities"] = self._safe_val(
            data.get("changeToOperatingActivities")
        )
        row["cash_netIncome"] = self._safe_val(data.get("netIncome"))
        row["changeInCash"] = self._safe_val(data.get("changeInCash"))
        row["beginPeriodCashFlow"] = self._safe_val(data.get("beginPeriodCashFlow"))
        row["endPeriodCashFlow"] = self._safe_val(data.get("endPeriodCashFlow"))
        row["totalCashFromOperatingActivities"] = self._safe_val(
            data.get("totalCashFromOperatingActivities")
        )
        row["depreciation"] = self._safe_val(data.get("depreciation"))
        row["otherCashflowsFromInvestingActivities"] = self._safe_val(
            data.get("otherCashflowsFromInvestingActivities")
        )
        row["dividendsPaid"] = self._safe_val(data.get("dividendsPaid"))
        row["changeToInventory"] = self._safe_val(data.get("changeToInventory"))
        row["changeToAccountReceivables"] = self._safe_val(
            data.get("changeToAccountReceivables")
        )
        row["salePurchaseOfStock"] = self._safe_val(data.get("salePurchaseOfStock"))
        row["otherCashflowsFromFinancingActivities"] = self._safe_val(
            data.get("otherCashflowsFromFinancingActivities")
        )
        row["changeToNetincome"] = self._safe_val(data.get("changeToNetincome"))
        row["capitalExpenditures"] = self._safe_val(data.get("capitalExpenditures"))
        row["changeReceivables"] = self._safe_val(data.get("changeReceivables"))
        row["cashFlowsOtherOperating"] = self._safe_val(
            data.get("cashFlowsOtherOperating")
        )
        row["exchangeRateChanges"] = self._safe_val(data.get("exchangeRateChanges"))
        row["cashAndCashEquivalentsChanges"] = self._safe_val(
            data.get("cashAndCashEquivalentsChanges")
        )
        row["changeInWorkingCapital"] = self._safe_val(
            data.get("changeInWorkingCapital")
        )
        row["otherNonCashItems"] = self._safe_val(data.get("otherNonCashItems"))
        row["freeCashFlow"] = self._safe_val(data.get("freeCashFlow"))

    def _extract_income_statement_fields(self, row: dict, data: dict, prefix=""):
        row[prefix + "date"] = self._safe_val(data.get("date"))
        row[prefix + "filing_date"] = self._safe_val(data.get("filing_date"))
        row["researchDevelopment"] = self._safe_val(data.get("researchDevelopment"))
        row["effectOfAccountingCharges"] = self._safe_val(
            data.get("effectOfAccountingCharges")
        )
        row["incomeBeforeTax"] = self._safe_val(data.get("incomeBeforeTax"))
        row["minorityInterest"] = self._safe_val(data.get("minorityInterest"))
        row["income_netIncome"] = self._safe_val(data.get("netIncome"))
        row["sellingGeneralAdministrative"] = self._safe_val(
            data.get("sellingGeneralAdministrative")
        )
        row["sellingAndMarketingExpenses"] = self._safe_val(
            data.get("sellingAndMarketingExpenses")
        )
        row["grossProfit"] = self._safe_val(data.get("grossProfit"))
        row["reconciledDepreciation"] = self._safe_val(
            data.get("reconciledDepreciation")
        )
        row["ebit"] = self._safe_val(data.get("ebit"))
        row["ebitda"] = self._safe_val(data.get("ebitda"))
        row["depreciationAndAmortization"] = self._safe_val(
            data.get("depreciationAndAmortization")
        )
        row["nonOperatingIncomeNetOther"] = self._safe_val(
            data.get("nonOperatingIncomeNetOther")
        )
        row["operatingIncome"] = self._safe_val(data.get("operatingIncome"))
        row["otherOperatingExpenses"] = self._safe_val(
            data.get("otherOperatingExpenses")
        )
        row["interestExpense"] = self._safe_val(data.get("interestExpense"))
        row["taxProvision"] = self._safe_val(data.get("taxProvision"))
        row["interestIncome"] = self._safe_val(data.get("interestIncome"))
        row["netInterestIncome"] = self._safe_val(data.get("netInterestIncome"))
        row["extraordinaryItems"] = self._safe_val(data.get("extraordinaryItems"))
        row["nonRecurring"] = self._safe_val(data.get("nonRecurring"))
        row["otherItems"] = self._safe_val(data.get("otherItems"))
        row["incomeTaxExpense"] = self._safe_val(data.get("incomeTaxExpense"))
        row["totalRevenue"] = self._safe_val(data.get("totalRevenue"))
        row["totalOperatingExpenses"] = self._safe_val(
            data.get("totalOperatingExpenses")
        )
        row["costOfRevenue"] = self._safe_val(data.get("costOfRevenue"))
        row["totalOtherIncomeExpenseNet"] = self._safe_val(
            data.get("totalOtherIncomeExpenseNet")
        )
        row["discontinuedOperations"] = self._safe_val(
            data.get("discontinuedOperations")
        )
        row["netIncomeFromContinuingOps"] = self._safe_val(
            data.get("netIncomeFromContinuingOps")
        )
        row["netIncomeApplicableToCommonShares"] = self._safe_val(
            data.get("netIncomeApplicableToCommonShares")
        )
        row["preferredStockAndOtherAdjustments"] = self._safe_val(
            data.get("preferredStockAndOtherAdjustments")
        )

    def _get_latest_financials(self, section: dict) -> dict:
        """Returns the dictionary with the largest date key
        from 'quarterly' or if none, from 'yearly'."""

        def pick_latest(d):
            if not d or not isinstance(d, dict):
                return {}
            try:
                max_date = max(d.keys())
                return d[max_date] if isinstance(d[max_date], dict) else {}
            except ValueError:
                return {}

        q = pick_latest(section.get("quarterly", {}))
        return q if q else pick_latest(section.get("yearly", {}))

    def _safe_val(self, x):
        return x if x is not None else None
