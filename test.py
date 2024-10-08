import pandas as pd


# Update rst_to_dataframe function to discard leading/trailing whitespace lines
def convert_rst_to_dataframe(rst_table):
    # Step 1: Discard pure whitespace lines at the start and end
    lines = [
        line.strip() for line in rst_table.splitlines() if line.strip()
    ]  # Trim and discard empty lines

    # Step 2: Parse the first line (row separator with '+' and '-')
    first_line = lines[0]
    if (
        not first_line.startswith("+")
        or not first_line.endswith("+")
        or "|" in first_line
    ):
        raise ValueError(
            "The first line should start and end with '+' and contain only '-' between '+' signs."
        )

    # Step 3: Number of columns is the number of '+'s minus 1
    colcnt = first_line.count("+") - 1

    # Step 4: Parse the second line (header content)
    second_line = lines[1]
    if not second_line.startswith("|") or not second_line.endswith("|"):
        raise ValueError("Header row should start and end with '|'")

    column_names = [col.strip() for col in second_line[1:-1].split("|")]
    if len(column_names) != colcnt:
        raise ValueError(f"Expected {colcnt} columns, but found {len(column_names)}")

    # Continue checking the header row for multiline column names
    i = 2
    while i < len(lines):
        current_line = lines[i]
        if current_line.startswith("+"):
            break  # Done collecting column names
        if current_line.startswith("|"):
            extra_parts = [col.strip() for col in current_line[1:-1].split("|")]
            if len(extra_parts) != colcnt:
                raise ValueError(
                    f"Expected {colcnt} columns, but found {len(extra_parts)}"
                )
            # Concat the new parts position-wise with the current column names
            column_names = [
                f"{old} {new}".strip() for old, new in zip(column_names, extra_parts)
            ]
        i += 1

    # Step 5: Parse the data rows
    data_rows = []
    current_row = []
    inside_data_row = False  # Track if we're in the middle of a multi-line data row

    for line in lines[i + 1 :]:
        if line.startswith("+") and inside_data_row:
            # End of a line-group (data row ends)
            while len(current_row) < colcnt:  # Ensure correct number of columns
                current_row.append("")
            data_rows.append(current_row)
            current_row = []  # Reset for the next row
            inside_data_row = False
        elif line.startswith("|"):
            inside_data_row = True  # We are inside a line-group (data row)
            row_cells = [cell.strip() for cell in line[1:-1].split("|")]
            if len(current_row) == 0:
                current_row = row_cells
            else:
                # Merge multiline content with the current row
                current_row = [
                    f"{a} {b}".strip() for a, b in zip(current_row, row_cells)
                ]

    # Step 6: Create a DataFrame using the parsed column names and data rows
    df = pd.DataFrame(data_rows, columns=column_names)

    return df


# Test input with whitespace lines
rst_input = """

    
    +------------+----------------------+-----------------------------------+------------------------+------------------------------+---------------------------------+
    | date       | new investment (x1k) | new investment distribution (x1k) | total investment (x1k) | total ownership distribution | notes                           |
    +============+======================+===================================+========================+==============================+=================================+
    | 2024-09-05 | 4.0                  | {Harsh: 3.4, Aish: 0.6}           | 82.9                   | {Harsh: 99.28%, Aish: 0.72%} | mortgage; Aish advance paid     |
    |            |                      |                                   |                        |                              | Harsh                           |
    +------------+----------------------+-----------------------------------+------------------------+------------------------------+---------------------------------+
    | 2024-09-01 | 0.3                  | {Harsh: 0.3, Aish: 0}             | 78.9                   | {Harsh: 100%, Aish: 0%}      | Amazon home goods (toilet rugs: |
    |            |                      |                                   |                        |                              | 70, Keurig + pods: 90, hallway  |
    |            |                      |                                   |                        |                              | rug: 100, salt n pepper         |
    |            |                      |                                   |                        |                              | grinders: 14)                   |
    +------------+----------------------+-----------------------------------+------------------------+------------------------------+---------------------------------+
    
"""

# Test the updated function with whitespace trimming at the start
df_final_with_trimmed_lines = convert_rst_to_dataframe(rst_input)
print(df_final_with_trimmed_lines)
