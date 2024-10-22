from text_table_tools import TextTableTools


class OwnershipTableTools:
    @staticmethod
    def ingest_undocumented_kharchas_to_ownership_table(
        ownership_table, undocd_kharchas
    ):
        return ownership_table + undocd_kharchas
