from docutils.core import publish_string
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import textwrap


class TextTableTools:
    @staticmethod
    def convert_texttable_to_dataframe(rst_text_table):
        """Convert text-table to Dataframe.

        EXAMPLE:

        ```
        from text_table_tools import TextTableTools

        text_table = '''
            +------------+----------------------+-----------------------------------+------------------------+------------------------------+---------------------------------+
            | date       | new investment (x1k) | new investment distribution (x1k) | total investment (x1k) | total ownership distribution | notes                           |
            +============+======================+===================================+========================+==============================+=================================+
            | 2024-09-05 | 4.0                  | {Lufko: 3.4, Blerk: 0.6}          | 82.9                   | {Lufko: 99.28%, Blerk: 0.72% | frundl; Blerk puvarg tolf       |
            |            |                      |                                   |                        |                              | Lufko                           |
            +------------+----------------------+-----------------------------------+------------------------+------------------------------+---------------------------------+
            | 2024-09-01 | 0.3                  | {Lufko: 0.3, Blerk: 0}            | 78.9                   | {Lufko: 100%, Blerk: 0%}     | Queldo nurn gormb (vorpik: 70,  |
            |            |                      |                                   |                        |                              | Wilsk + kleps: 90, klunkop      |
            |            |                      |                                   |                        |                              | grosh: 100, kip tuflin: 14)     |
            +------------+----------------------+-----------------------------------+------------------------+------------------------------+---------------------------------+

            '''
        df = TextTableTools.convert_texttable_to_dataframe(text_table)
        print(df)
        ```

        # Output:
        #          date new investment (x1k)  ...  total ownership distribution                                              notes
        # 0  2024-09-05                  4.0  ...  {Lufko: 99.28%, Blerk: 0.72%                    frundl; Blerk puvarg tolf Lufko
        # 1  2024-09-01                  0.3  ...      {Lufko: 100%, Blerk: 0%}  Queldo nurn gormb (vorpik: 70, Wilsk + kleps: ...

        """

        # Step 1: Discard pure whitespace lines at the start and end
        lines = [
            line.strip() for line in rst_text_table.splitlines() if line.strip()
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
            raise ValueError(
                f"Expected {colcnt} columns, but found {len(column_names)}"
            )

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
                    f"{old} {new}".strip()
                    for old, new in zip(column_names, extra_parts)
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

    @staticmethod
    def convert_dataframe_to_texttable(df, max_width=30):
        """Convert Dataframe to text-table.

        EXAMPLE:

        ```
        from text_table_tools import TextTableTools
        import pandas as pd

        df_json = '''
            {
                "columns": ["name", "age", "city"],
                "index": [0, 1, 2],
                "data": [
                    ["Alice", 25, "New York"],
                    ["Bob", 30, "Los Angeles"],
                    ["Charlie", 35, "Chicago is such a great city. Why are we talking about the city again?"]
                ]
            }
            '''
        df = pd.read_json(df_json, orient="split")
        text_table = TextTableTools.convert_dataframe_to_texttable(df, max_width=25)
        print(text_table)
        ```

        # Output:
        # +---------+-----+---------------------------+
        # | name    | age | city                      |
        # +=========+=====+===========================+
        # | Alice   | 25  | New York                  |
        # +---------+-----+---------------------------+
        # | Bob     | 30  | Los Angeles               |
        # +---------+-----+---------------------------+
        # | Charlie | 35  | Chicago is such a great   |
        # |         |     | city. Why are we talking  |
        # |         |     | about the city again?     |
        # +---------+-----+---------------------------+

        """

        # Get column headers and determine column widths

        def wrap_text(text, width):
            return textwrap.fill(
                text, break_long_words=False, break_on_hyphens=False, width=width
            )

        def get_ideal_col_width(df, col, max_width=max_width):

            # Get raw widths of all cells including header
            raw_widths = [len(str(x)) for x in df[col]]
            raw_widths.append(len(col))

            # Compute typical cell width at the higher end
            pHi_width = int(np.percentile(raw_widths, 70))

            # Compare with max_width and set initial col_width
            col_width = min(pHi_width, max_width)

            # Wrap text for all col cells and set col_width to width of widest cell

            def get_wrapped_width(text, width):
                lines = wrap_text(text, width).split("\n")
                return max(len(line) for line in lines) if lines else 0

            wrapped_widths = [get_wrapped_width(row, col_width) for row in df[col]] + [
                get_wrapped_width(col, col_width)
            ]
            col_width = max(wrapped_widths)

            return col_width

        # Get column headers and determine column widths
        col_widths = {col: get_ideal_col_width(df, col) for col in df.columns}

        # Prepare the table lines
        lines = []

        # Header row with wrapping
        wrapped_headers = [
            wrap_text(col, col_widths[col]).split("\n") for col in df.columns
        ]
        max_header_lines = max([len(header) for header in wrapped_headers])
        header_lines = [""] * max_header_lines

        for i in range(max_header_lines):
            for j, header in enumerate(wrapped_headers):
                if i < len(header):
                    header_lines[
                        i
                    ] += f"| {header[i].ljust(col_widths[df.columns[j]])} "
                else:
                    header_lines[i] += f"| {' '.ljust(col_widths[df.columns[j]])} "
            header_lines[i] += "|"  # Ensure '|' at the end

        separator_row = (
            "+-" + "-+-".join(["-" * col_widths[col] for col in df.columns]) + "-+"
        )
        equal_row = (
            "+=" + "=+=".join(["=" * col_widths[col] for col in df.columns]) + "=+"
        )

        # Add header and separator
        lines.append(separator_row)
        lines.extend(header_lines)
        lines.append(equal_row)

        # Add data rows
        for _, row in df.iterrows():
            row_lines = [""] * max(
                [
                    len(wrap_text(row[col], col_widths[col]).split("\n"))
                    for col in df.columns
                ]
            )
            for col in df.columns:
                wrapped_col = wrap_text(row[col], col_widths[col]).split("\n")
                for i in range(len(row_lines)):
                    if i < len(wrapped_col):
                        row_lines[i] += f"| {wrapped_col[i].ljust(col_widths[col])} "
                    else:
                        row_lines[i] += f"| {' '.ljust(col_widths[col])} "
            row_lines = [
                row_line + "|" for row_line in row_lines
            ]  # Ensure '|' at the end of each row

            for r in row_lines:
                lines.append(r)
            lines.append(separator_row)

        # Return the final table as a string
        return "\n".join(lines)

    @staticmethod
    def convert_dataframe_to_html_table(df):
        """
        Convert a Pandas DataFrame into a HTML table.

        EXAMPLE:

        ```
        from text_table_tools import TextTableTools
        import pandas as pd

        df_json = '''
            {
                "columns": ["name", "age", "city"],
                "index": [0, 1, 2],
                "data": [
                    ["Alice", 25, "New York"],
                    ["Bob", 30, "Los Angeles"],
                    ["Charlie", 35, "Chicago is soo nice"]
                ]
            }
            '''
        df = pd.read_json(df_json, orient="split")
        html_table = TextTableTools.convert_dataframe_to_html_table(df)
        print(html_table)
        ```

        # Output:
        # <table border="1" style="border-collapse: collapse; width: 600px; text-align: left; font-size: 7pt; font-family: sans-serif;">
        # <thead>
        # <tr>
        # <th>name</th>
        # <th>age</th>
        # <th>city</th>
        # </tr>
        # </thead>
        # <tbody>
        # <tr>
        # <td>Alice</td>
        # <td>25</td>
        # <td>New York</td>
        # </tr>
        # <tr>
        # <td>Bob</td>
        # <td>30</td>
        # <td>Los Angeles</td>
        # </tr>
        # <tr>
        # <td>Charlie</td>
        # <td>35</td>
        # <td>Chicago is soo nice</td>
        # </tr>
        # </tbody>
        # </table>
        """

        # Convert the DataFrame to an HTML table
        full_html = df.to_html(index=False, border=1, escape=False)

        # Use BeautifulSoup to add custom attributes and simplify the table
        soup = BeautifulSoup(full_html, "html.parser")
        table = soup.find("table")

        # Add custom attributes to the table
        table.attrs = {
            "border": "1",
            "style": "border-collapse: collapse; width: 600px; text-align: left; font-size: 7pt; font-family: sans-serif;",
        }

        # Remove all attributes from child tags (like <th>, <td>)
        for tag in table.find_all(True):
            tag.attrs = {}

        return str(table)

    @staticmethod
    def convert_html_table_to_dataframe(html_table):
        """
        Convert an HTML table into a Pandas DataFrame.
        """
        
        # Parse the HTML table
        soup = BeautifulSoup(html_table, "html.parser")
        table = soup.find("table")  # Locate the table

        # Extract headers: Prioritize <th> over <td> in <thead>
        headers = []
        thead = table.find("thead")
        if thead:
            headers = [th.text.strip() for th in thead.find_all("th")]
            if not headers:  # Fallback to <td> if no <th> is found
                headers = [td.text.strip() for td in thead.find_all("td")]

        # Extract rows
        rows = []
        for tr in table.find_all("tr"):
            cells = tr.find_all(["td", "th"])  # Include both <td> and <th> for row data
            row = [cell.text.strip() for cell in cells]
            rows.append(row)

        # Remove header row from rows if headers exist
        if headers and rows:
            rows = rows[1:]  # Skip the first row if it matches headers

        # Create a DataFrame
        df = pd.DataFrame(rows, columns=headers if headers else None)
        return df