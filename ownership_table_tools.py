from text_table_tools import TextTableTools
import json
import pandas as pd
import re


class OwnershipTableTools:
    @staticmethod
    def ingest_undocumented_kharchas_to_ownership_table(
        ownership_table, undocd_kharchas, rebuild_table_from_scratch=False
    ):
        # init result df: new_odf
        new_odf = pd.DataFrame(
            columns=[
                "date",
                "new investment (x1k)",
                "new investment distribution (x1k)",
                "total investment (x1k)",
                "total investment distribution (x1k)",
                "total ownership distribution",
                "notes",
            ]
        ).astype(str)

        # load ownership_table into a df: old_odf
        old_odf = (
            TextTableTools.convert_texttable_to_dataframe(ownership_table)
            .sort_values("date")
            .reset_index(drop=True)
        )

        # init dict to hold running total investment distro: tot_inv_distro_dict
        tot_inv_distro_dict = dict()

        # add undocd kharchas to new_odf
        if rebuild_table_from_scratch:
            for _, investment_ser in old_odf.iterrows():
                # TODO: change this step to just get a list of inv_dicts; we need to combine these with undocd_kharchas and sort by date before ingesting
                OwnershipTableTools.ingest_single_investment_to_ownership_df(
                    new_odf, investment_ser.to_dict(), tot_inv_distro_dict
                )
        else:
            new_odf = pd.concat(
                [
                    new_odf,
                    old_odf.reindex(columns=new_odf.columns),
                ],
                ignore_index=True,
            )

            # TODO: update tot_inv_distro_dict from last row of old_odf; if fail, raise error with reco to try again with rebuild

        # convert new_odf back to text table: o_ttable_new
        new_odf = new_odf.iloc[::-1].reset_index(drop=True)
        o_ttable_new = TextTableTools.convert_dataframe_to_texttable(new_odf)

        return o_ttable_new

    @staticmethod
    def ingest_single_investment_to_ownership_df(
        ownership_df, investment_dict, tot_inv_distro_dict
    ):
        # extract date
        date = str(pd.to_datetime(investment_dict["date"]))[0:10]
        # TODO: raise error if date is older than last (newest) date in ownership_df

        # extract investment distro dict: inv_dist_dict
        inv_dist_jsonlike = investment_dict["new investment distribution (x1k)"].lower()
        inv_dist_dict = None
        try:
            parsed_dict = json.loads(inv_dist_jsonlike)
            if all(
                isinstance(k, str) and isinstance(v, (float, int))
                for k, v in parsed_dict.items()
            ):
                # Convert int values to float if needed
                inv_dist_dict = {k: float(v) for k, v in parsed_dict.items()}
        except:
            inv_dist_jsonlike = re.sub(r"(\w+):", r'"\1":', inv_dist_jsonlike)
            inv_dist_dict = {k: float(v) for k, v in eval(inv_dist_jsonlike).items()}
        inv_dist_dict = dict(sorted(inv_dist_dict.items()))

        # extract note
        note = investment_dict["notes"]

        # raise error if exact same (date, inv_dist_dict) is already there in ownership_df
        inv_dist_json = json.dumps(inv_dist_dict)
        if (
            f"{date} {inv_dist_json}"
            in (
                ownership_df["date"]
                + " "
                + ownership_df["new investment distribution (x1k)"]
            ).tolist()
        ):
            raise ValueError(
                f"Investment already exists in ownership_df with (date, inv_dist) = ({date}, {inv_dist_json})"
            )

        # compute new investment: new_inv
        new_inv = sum(list(inv_dist_dict.values()))

        # update total investment distro: tot_inv_distro_dict
        for person, amount in inv_dist_dict:
            tot_inv_distro_dict[person] = (
                tot_inv_distro_dict.get(person, 0) + inv_dist_dict[person]
            )

        pass

    @staticmethod
    def convert_undocd_kharcha_text_to_investment_dict(undocd_kharcha_text):
        return dict()


if __name__ == "__main__":
    ownership_table = """
        +---------------+-------------------------+--------------------------------------+---------------------------+---------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        |               |                         |                                      |                           |                                 |                                                                                                                                                                                                             |
        | date          | new investment (x1k)    | new investment distribution (x1k)    | total investment (x1k)    | total ownership distribution    | notes                                                                                                                                                                                                       |
        +---------------+-------------------------+--------------------------------------+---------------------------+---------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        |               |                         |                                      |                           |                                 |                                                                                                                                                                                                             |
        | 2024-09-05    | 4                       | {Harsh: 3.4, Aish: 0.6}              | 82.9                      | {Harsh: 99.28%, Aish: 0.72%}    | mortgage; Aish advance paid Harsh                                                                                                                                                                           |
        +---------------+-------------------------+--------------------------------------+---------------------------+---------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        |               |                         |                                      |                           |                                 |                                                                                                                                                                                                             |
        | 2024-09-01    | 0.3                     | {Harsh: 0.3, Aish: 0}                | 78.9                      | {Harsh: 100%, Aish: 0%}         | Amazon home goods (toilet rugs: 70,   Keurig + pods: 90, hallway rug: 100, salt n pepper grinders: 14)                                                                                                      |
        +---------------+-------------------------+--------------------------------------+---------------------------+---------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        |               |                         |                                      |                           |                                 |                                                                                                                                                                                                             |
        | 2024-08-01    | 4                       | {Harsh: 4, Aish: 0}                  | 78.6                      | {Harsh: 100%, Aish: 0%}         | mortgage                                                                                                                                                                                                    |
        +---------------+-------------------------+--------------------------------------+---------------------------+---------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        |               |                         |                                      |                           |                                 |                                                                                                                                                                                                             |
        | 2024-07-13    | 1.6                     | {Harsh: 1.6, Aish: 0}                | 74.6                      | {Harsh: 100%, Aish: 0%}         | Amazon   furniture (bed frame x3 + rug: 0.32, standing desk x3: 0.28, mattress +   mattress-protector x3: 0.21, desk x3: 0.11, patio table: 0.12, patio chairs:   0.12, rug x3: 0.31, rug cleaner: 0.10)    |
        +---------------+-------------------------+--------------------------------------+---------------------------+---------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        |               |                         |                                      |                           |                                 |                                                                                                                                                                                                             |
        | 2024-07-12    | 0.5                     | {Harsh: 0.5, Aish: 0}                | 73                        | {Harsh: 100%, Aish: 0%}         | home inspection                                                                                                                                                                                             |
        +---------------+-------------------------+--------------------------------------+---------------------------+---------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        |               |                         |                                      |                           |                                 |                                                                                                                                                                                                             |
        | 2024-06-28    | 0.9                     | {Harsh: 0.9, Aish: 0}                | 72.5                      | {Harsh: 100%, Aish: 0%}         | Habitat furniture                                                                                                                                                                                           |
        +---------------+-------------------------+--------------------------------------+---------------------------+---------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        |               |                         |                                      |                           |                                 |                                                                                                                                                                                                             |
        | 2024-06-28    | 0.6                     | {Harsh: 0.6, Aish: 0}                | 71.6                      | {Harsh: 100%, Aish: 0%}         | YMCA furniture                                                                                                                                                                                              |
        +---------------+-------------------------+--------------------------------------+---------------------------+---------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        |               |                         |                                      |                           |                                 |                                                                                                                                                                                                             |
        | 2024-06-27    | 46                      | {Harsh: 46, Aish: 0}                 | 71                        | {Harsh: 100%, Aish: 0%}         | closing cash payment (down payment   remainder etc)                                                                                                                                                         |
        +---------------+-------------------------+--------------------------------------+---------------------------+---------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        |               |                         |                                      |                           |                                 |                                                                                                                                                                                                             |
        | 2024-05-25    | 25                      | {Harsh: 25, Aish: 0}                 | 25                        | {Harsh: 100%, Aish: 0%}         | good faith money paid to escrow of   realtor Brian McHone                                                                                                                                                   |
        +---------------+-------------------------+--------------------------------------+---------------------------+---------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        """
    undocd_kharchas = ""
    ownership_table_new = (
        OwnershipTableTools.ingest_undocumented_kharchas_to_ownership_table(
            ownership_table, undocd_kharchas, rebuild_table_from_scratch=True
        )
    )
    print(ownership_table_new)

    print("whoa")
