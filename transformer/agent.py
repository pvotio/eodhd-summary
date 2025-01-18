from datetime import datetime

import pandas as pd


class Agent:
    FIELDS = {
        # "etl.eodhd_analyst_ratings": [
        #     "AnalystRatings",
        #     [
        #         "Rating",
        #         "TargetPrice",
        #         "StrongBuy",
        #         "Buy",
        #         "Hold",
        #         "Sell",
        #         "StrongSell",
        #     ],
        # ],
        "etl.eodhd_basics": [
            "#",
            [
                "General::FullTimeEmployees#FullTimeEmployees",
                "General::UpdatedAt#UpdatedAt",
                "General::AddressData::ZIP#ZIP",
                "General::AddressData::Country#Country",
                "General::AddressData::City#City",
                "General::Sector#Sector",
                "General::Industry#Industry",
                "General::GicSector#GicSector",
                "General::GicIndustry#GicIndustry",
            ],
        ],
        "etl.eodhd_esg_scores": [
            "ESGScores",
            [
                "RatingDate",
                "TotalEsg",
                "TotalEsgPercentile",
                "EnvironmentScore",
                "EnvironmentScorePercentile",
                "GovernanceScore",
                "GovernanceScorePercentile",
                "ControversyLevel",
                "SocialScore",
                "SocialScorePercentile",
            ],
        ],
        "etl.eodhd_esg_scores_activities": [
            "ESGScores&ActivitiesInvolvement",
            [
                "adult",
                "alcoholic",
                "animalTesting",
                "catholic",
                "controversialWeapons",
                "smallArms",
                "furLeather",
                "gambling",
                "gmo",
                "militaryContract",
                "nuclear",
                "pesticides",
                "palmOil",
                "coal",
                "tobacco",
            ],
        ],
        "etl.eodhd_highlights": [
            "Highlights",
            [
                "EBITDA",
                "PERatio",
                "PEGRatio",
                "WallStreetTargetPrice",
                "BookValue",
                "DividendShare",
                "DividendYield",
                "EarningsShare",
                "EPSEstimateCurrentYear",
                "EPSEstimateNextYear",
                "EPSEstimateNextQuarter",
                "EPSEstimateCurrentQuarter",
                "MostRecentQuarter",
                "ProfitMargin",
                "OperatingMarginTTM",
                "ReturnOnAssetsTTM",
                "ReturnOnEquityTTM",
                "RevenueTTM",
                "RevenuePerShareTTM",
                "QuarterlyRevenueGrowthYOY",
                "GrossProfitTTM",
                "DilutedEpsTTM",
                "QuarterlyEarningsGrowthYOY",
            ],
        ],
        "etl.eodhd_valuation": [
            "Valuation",
            [
                "TrailingPE",
                "ForwardPE",
                "PriceSalesTTM",
                "PriceBookMRQ",
                "EnterpriseValueRevenue",
                "EnterpriseValueEbitda",
            ],
        ],
        "etl.eodhd_shares_stats": [
            "SharesStats",
            [
                "SharesOutstanding",
                "SharesFloat",
                "SharesShortPriorMonth",
                "ShortRatio",
                "ShortPercentOutstanding",
                "ShortPercentFloat",
            ],
        ],
        "etl.eodhd_dividends": [
            "#",
            [
                "SplitsDividends::ForwardAnnualDividendRate#ForwardAnnualDividendRate",
                "SplitsDividends::ForwardAnnualDividendYield#ForwardAnnualDividendYield",
                "SplitsDividends::PayoutRatio#PayoutRatio",
            ],
        ],
        "etl.eodhd_balance": ["#", []],
    }

    def __init__(self, data):
        self.data = data

    def transform(self):
        result = {}

        for ticker in self.data:
            if not self.data[ticker]:
                continue

            for table in self.FIELDS:
                if not table in result:
                    result[table] = []

                row = {"bbg_comp_ticker": ticker}

                if table == "etl.eodhd_esg_scores_activities":
                    if (
                        "ESGScores" in self.data[ticker]
                        and self.data[ticker]["ESGScores"] != "NA"
                        and "ActivitiesInvolvement" in self.data[ticker]["ESGScores"]
                        and self.data[ticker]["ESGScores"]["ActivitiesInvolvement"]
                        != "NA"
                    ):

                        ticker_data = self.data[ticker]["ESGScores"][
                            "ActivitiesInvolvement"
                        ]

                        for i in ticker_data:
                            _d_ = ticker_data[i]
                            activity = _d_["Activity"]
                            involvement = _d_["Involvement"]

                            if activity in self.FIELDS[table][1]:
                                row[activity] = involvement

                    else:
                        for k in self.FIELDS[table][1]:
                            row[k] = None

                    row["timestamp_created_utc"] = self.timenow()
                    result[table].append(row)
                    continue

                elif table == "etl.eodhd_balance":

                    if (
                        "Financials::Balance_Sheet" in self.data[ticker]
                        or "Financials" in self.data[ticker]
                    ):
                        if "Financials" in self.data[ticker]:
                            balance = self.data[ticker]["Financials"]["Balance_Sheet"]
                        else:
                            balance = self.data[ticker]["Financials::Balance_Sheet"]

                        if not "quarterly" in balance:
                            continue

                        data = balance["quarterly"]
                        if len(data) == 0:
                            continue

                        for i in range(2):
                            _row = row.copy()

                            try:
                                key = list(data.keys())[i]
                                _data = data[key]
                            except:
                                break

                            if _data is None:
                                continue

                            for k, v in _data.items():
                                _row[k] = self.valcheck(v, date="date" in k)

                            if len(_row) > 2:
                                _row["timestamp_created_utc"] = self.timenow()
                                result[table].append(_row)

                    continue

                column = self.FIELDS[table][0]
                if column != "#":
                    if (
                        column in self.data[ticker]
                        and self.data[ticker][column] != "NA"
                    ):
                        ticker_data = self.data[ticker][column]
                    else:
                        ticker_data = {}

                    for k in self.FIELDS[table][1]:
                        if "#" in k:
                            k, name = k.split("#")
                        else:
                            name = k

                        if k in ticker_data:
                            row[name] = self.valcheck(
                                ticker_data[k], date="date" in name
                            )
                        else:
                            row[name] = None

                    row["timestamp_created_utc"] = self.timenow()
                    result[table].append(row)

                else:
                    columns = self.FIELDS[table][1]
                    for c in columns:
                        column = c.split("#")[0]
                        key = c.split("#")[1]

                        if column in self.data[ticker]:
                            if "date" in key.lower() or "UpdatedAt" in key.lower():
                                is_date = True
                            else:
                                is_date = False

                            row[key] = self.valcheck(
                                self.data[ticker][column], date=is_date
                            )
                        else:
                            row[key] = None

                    row["timestamp_created_utc"] = self.timenow()
                    result[table].append(row)

        combine = {
            "etl.eodhd_fundamentals": [
                "etl.eodhd_dividends",
                "etl.eodhd_valuation",
                "etl.eodhd_highlights",
            ]
        }

        if combine != {}:
            for key in combine:
                if len(combine[key]) != 0:
                    result[key] = []
                    table = result[combine[key][0]]
                    for row in table:
                        ticker = row["bbg_comp_ticker"]
                        newrow = {"bbg_comp_ticker": ticker}

                        for _key in row:
                            if _key in newrow.keys():
                                continue

                            newrow[_key] = row[_key]

                        if len(combine[key]) > 1:
                            for _table in combine[key][1:]:
                                exrow = self.find_ticker(ticker, result[_table])
                                if not exrow:
                                    continue

                                for _key in exrow:
                                    if _key in newrow.keys():
                                        continue

                                    newrow[_key] = exrow[_key]

                        del newrow["timestamp_created_utc"]
                        newrow["timestamp_created_utc"] = self.timenow()
                        result[key].append(newrow)

                    for table in combine[key]:
                        if table in result:
                            del result[table]

        result = {t: pd.DataFrame(data) for t, data in result.items()}
        return result

    @staticmethod
    def valcheck(value, date):
        if value in ["NA", "NaN", "", 0, "0", None]:
            if date:
                return None

            return None

        if date:
            try:
                x = datetime.strptime(value, "%Y-%m-%d")
                if x.year < 1900:
                    return None

                return value
            except:
                return None

        if isinstance(value, str):
            try:
                return round(float(value), 4)
            except:
                return value

        if isinstance(value, int):
            return round(float(value), 4)

        else:
            return value

    @staticmethod
    def timenow():
        return datetime.utcnow()

    @staticmethod
    def find_ticker(ticker, data: list):
        for row in data:
            if row["bbg_comp_ticker"] == ticker:
                return row

        return False
