import io
import pandas as pd


class ExpensesTableTools:
    @staticmethod
    def update_todate_cols_in_expenses_table(expenses_csv):

        # read the csv string into a pandas dataframe: expenses_df
        expenses_df = pd.read_csv(io.StringIO(expenses_csv))

        # update the todate columns in expenses_df
        expenses_df = expenses_df.sort_values(by=["date"], ascending=True)
        expenses_df["spend (x1k USD)"] = expenses_df["spend (x1k USD)"].astype(float)
        cumulative_spend = 0
        cumulative_split = {}
        todate_spends = []
        todate_spend_splits = []
        todate_ownership_percent_splits = []
        for _, row in expenses_df.iterrows():
            spender = row["spender"]
            spend = row["spend (x1k USD)"]
            cumulative_spend = round(cumulative_spend + spend, 2)
            todate_spends.append(cumulative_spend)
            cumulative_split[spender] = round(
                cumulative_split.get(spender, 0) + spend, 2
            )
            todate_spend_splits.append(cumulative_split.copy())
            todate_ownership_percent_splits.append(
                {
                    spender: round(
                        cumulative_split[spender] / cumulative_spend * 100, 1
                    )
                    for spender in cumulative_split
                }
            )
        expenses_df["todate_spend (x1k USD)"] = todate_spends
        expenses_df["todate_spend_split (x1k USD)"] = todate_spend_splits
        expenses_df["todate_ownership_percent_split"] = todate_ownership_percent_splits
        expenses_df = expenses_df.sort_values(by=["date"], ascending=False)

        # convert updated expenses_df to csv string: expenses_new_csv
        expenses_new_csv = (
            expenses_df.astype(str).replace("nan", "").to_csv(index=False, header=True)
        )

        return expenses_new_csv
