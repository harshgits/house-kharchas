from flask import Flask, request, render_template
from ownership_table_tools import OwnershipTableTools as OTT

app = Flask(__name__)


@app.route("/", methods=["GET", "POST"])
def input_form():
    ownership_table_new = None  # Initialize the result as None
    if request.method == "POST":
        # Get the input values from the form
        undocd_kharchas = request.form["undocd_kharchas"]
        ownership_table = request.form["ownership_table"]
        rebuild_table = "rebuild_table" in request.form

        # compute result
        ownership_table_new = OTT.ingest_undocumented_kharchas_to_ownership_table(
            ownership_table, undocd_kharchas, rebuild_table
        )

    # Render the template, pass the concatenated result to the template
    return render_template("webpage.html", ownership_table_new=ownership_table_new)


if __name__ == "__main__":
    # app.run(host="0.0.0.0", port=5000)
    app.run(debug=True)
