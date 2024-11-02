from text_table_tools import TextTableTools
import json
import pandas as pd
import re


class OwnershipTableTools:
    @staticmethod
    def ingest_undocumented_kharchas_to_ownership_table(
        ownership_table, undocd_kharchas_text, rebuild_table_from_scratch=False
    ):
        # init result df: new_odf
        new_odf = pd.DataFrame(
            columns=[
                "date",
                "total ownership distribution",
                "new investment (x1k)",
                "notes",
                "new investment distribution (x1k)",
                "total investment (x1k)",
                "total investment distribution (x1k)",
            ]
        ).astype(str)

        # load ownership_table into a df: old_odf
        old_odf = (
            TextTableTools.convert_texttable_to_dataframe(ownership_table)
            .iloc[::-1]
            .reset_index(drop=True)
        )

        # init dict to hold running total investment distro: tot_inv_distro_dict
        tot_inv_distro_dict = dict()

        # init list to hold investment dicts that will populate new_odf: inv_dicts
        # - also we maintain a set of inv_dict unique ids of format (inv_date, inv_distro)
        #   to eliminate dups: inv_dict_ids
        inv_dicts = []
        inv_dict_ids = set()

        # extract things from old_odf: tot_inv_distro_dict (if needed), inv_dicts (if needed)
        def convert_inv_distro_jsonlike_to_inv_distro_dict(inv_dist_jsonlike):
            # init result
            inv_dist_dict = None

            # parse inv_dist_jsonlike into inv_dist_dict
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
                inv_dist_dict = {
                    k: float(v) for k, v in eval(inv_dist_jsonlike).items()
                }

            # remove inv_dist_dict items where amount is 0
            inv_dist_dict = {k: float(v) for k, v in inv_dist_dict.items() if v > 0}

            # sort inv_dist_dict
            inv_dist_dict = dict(sorted(inv_dist_dict.items()))

            return inv_dist_dict

        def add_invdict_to_invdicts(inv_dict, inv_dicts, inv_dict_ids):
            inv_dict_id = (
                inv_dict["date"],
                json.dumps(inv_dict["inv_distro_dict"]),
            )
            if inv_dict_id not in inv_dict_ids:
                inv_dict_ids.add(inv_dict_id)
                inv_dicts.append(inv_dict)
            else:
                raise ValueError(
                    f"Tried adding existing investment to inv_dicts; investment-id:"
                    f"\n{inv_dict_id}"
                )

        if rebuild_table_from_scratch:
            for _, inv_ser in old_odf.iterrows():
                inv_dict = dict()
                inv_dict["date"] = pd.to_datetime(inv_ser["date"])
                inv_dict["inv_distro_dict"] = (
                    convert_inv_distro_jsonlike_to_inv_distro_dict(
                        inv_ser["new investment distribution (x1k)"].lower()
                    )
                )
                inv_dict["note"] = inv_ser["notes"]
                add_invdict_to_invdicts(inv_dict, inv_dicts, inv_dict_ids)
        else:
            try:
                tot_inv_distro_dict = convert_inv_distro_jsonlike_to_inv_distro_dict(
                    old_odf.iloc[-1]["total investment distribution (x1k)"].lower()
                )
            except Exception as e:
                raise ValueError(
                    f"Parsing tot_inv_distro_dict from old_odf failed with error (details ahead)"
                    f"; perhaps try with rebuild-table-from-scratch; error details:"
                    f"\n\n{e}"
                )
            new_odf = pd.concat(
                [
                    new_odf,
                    old_odf.reindex(columns=new_odf.columns),
                ],
                ignore_index=True,
            )

        # extract inv-dicts from undocd_kharchas_text into inv_dicts
        raw_kharchas = [
            block.strip()
            for block in re.split(r"\n\s*\n", undocd_kharchas_text.strip())
            if block.strip()
        ]

        def convert_kharcha_to_inv_dict(kharcha_text):
            # Split the input into lines and strip any leading/trailing spaces
            lines = kharcha_text.strip().split("\n")

            # Parse the first line for the date, amount, and recipient
            header_match = re.match(
                r"(\d{4}-\d{2}-\d{2})\.\s+(\d+\.\d+)k\s+\|\s+(\w+)", lines[0].strip()
            )

            if not header_match:
                raise ValueError("Invalid format for the header line.")

            # Extract parsed components
            date_str = header_match.group(1)
            amount = float(header_match.group(2))
            recipient = header_match.group(3).lower()  # Make the key lowercase

            # Create the dictionary structure
            inv_dict = {
                "date": pd.Timestamp(date_str),
                "inv_distro_dict": {recipient: amount},
                "note": lines[1].strip().strip("()") if len(lines) > 1 else "",
            }

            return inv_dict

        for kharcha in raw_kharchas:
            add_invdict_to_invdicts(
                convert_kharcha_to_inv_dict(kharcha), inv_dicts, inv_dict_ids
            )

        # sort inv_dicts by date
        inv_dicts = sorted(
            inv_dicts, key=lambda x: (x["date"], json.dumps(x["inv_distro_dict"]))
        )

        # ingest inv_dicts into new_odf
        def ingest_single_investment_to_ownership_df(
            odf, inv_dict, tot_inv_distro_dict
        ):
            # init row dict to be ingested: inv_odf_row_dict
            inv_odf_row_dict = dict()

            # raise error if date being ingested is older than odf last date
            if len(odf) > 0 and inv_dict["date"] < pd.to_datetime(odf.iloc[-1]["date"]):
                raise ValueError(
                    f"investment-date is older than last ownership_df date"
                    f"; perhaps try with rebuild-table-from-scratch"
                )

            # compute row.date
            inv_odf_row_dict["date"] = str(inv_dict["date"])[0:10]

            # compute row.new_inv_distro
            inv_distro_dict = pd.Series(inv_dict["inv_distro_dict"]).round(1).to_dict()
            inv_odf_row_dict["new investment distribution (x1k)"] = json.dumps(
                inv_distro_dict
            )

            # compute row.new_investment
            inv_odf_row_dict["new investment (x1k)"] = json.dumps(
                round(sum(inv_distro_dict.values()), 1)
            )

            # compute row.total_inv_distro
            for person, amount in inv_distro_dict.items():
                tot_inv_distro_dict[person] = (
                    tot_inv_distro_dict.get(person, 0) + amount
                )
            tot_inv_distro_dict = pd.Series(tot_inv_distro_dict).round(1).to_dict()
            tot_inv_distro_dict = dict(sorted(tot_inv_distro_dict.items()))
            inv_odf_row_dict["total investment distribution (x1k)"] = json.dumps(
                tot_inv_distro_dict
            )

            # compute row.total_inv
            tot_inv = round(sum(tot_inv_distro_dict.values()), 1)
            inv_odf_row_dict["total investment (x1k)"] = json.dumps(tot_inv)

            # compute row.total_ownp_dist
            tot_ownp_dist = {
                k: f"{(v / tot_inv * 100):.2f}".rstrip("0").rstrip(".") + "%"
                for k, v in tot_inv_distro_dict.items()
            }
            inv_odf_row_dict["total ownership distribution"] = json.dumps(tot_ownp_dist)

            # compute row.note
            inv_odf_row_dict["notes"] = re.sub(r"\s+", " ", inv_dict["note"].strip())

            # append inv_odf_row_dict to odf
            odf.loc[len(odf)] = inv_odf_row_dict

        for inv_dict in inv_dicts:
            ingest_single_investment_to_ownership_df(
                new_odf, inv_dict, tot_inv_distro_dict
            )

        # convert new_odf back to text table: o_ttable_new
        o_ttable_new = TextTableTools.convert_dataframe_to_texttable(
            new_odf.iloc[::-1].reset_index(drop=True)
        )

        return o_ttable_new

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
    undocd_kharchas = """
        2024-09-27. 0.3k | Aish
        (Airbnb Carolyn cash to Harsh)

        2024-09-26. 0.15k | Harsh
        (Electric bill)

        2024-09-24. 0.1k | Harsh
        (Utility bill)

        2024-09-23. 0.8k | Aish
        (Airbnb Shelly cash to Harsh)

        2024-09-24. 0.6k | Harsh
        (Humaji card: 70, Alicia clean July: 120, Amazon dish soap + clothes soap: 80Coffee pod holder: 20, Rug: 20, Fitted sheet: 30, Side table + hook rack x3 + putty: 90, Knives + pots + chopboard: 135, Keurig pods: 35)

        2024-09-04. 0.25k |   Harsh   
        (Amazon home goods: rollout bed, comforter, first aid kit, fitted sheet)
        """
    ownership_table_new = (
        OwnershipTableTools.ingest_undocumented_kharchas_to_ownership_table(
            ownership_table, undocd_kharchas, rebuild_table_from_scratch=True
        )
    )
    print(ownership_table_new)

    print("whoa")
