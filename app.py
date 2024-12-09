from flask import Flask, request, render_template
from ownership_table_tools import OwnershipTableTools as OTT

app = Flask(__name__)


@app.route("/", methods=["GET", "POST"])
def input_form():

    # initialize form inputs
    ownership_table_new_text = None
    ownership_table_new_html = None

    if request.method == "POST":

        # Get the input values from the form
        undocd_kharchas = request.form["undocd_kharchas"]
        ownership_table = request.form["ownership_table"]
        rebuild_table = "rebuild_table" in request.form

        # compute result
        try:
            ownership_table_new_text, extras = (
                OTT.ingest_undocumented_kharchas_to_ownership_table(
                    ownership_table, undocd_kharchas, rebuild_table
                )
            )
            ownership_table_new_html = extras["o_htmltable_new"]
        except Exception as e:
            ownership_table_new_text = f"Error occured during ingestion: \n{e}"

    # Render the template, pass the concatenated result to the template
    return render_template(
        "webpage.html",
        ownership_table_new_text=ownership_table_new_text,
        ownership_table_new_html=ownership_table_new_html,
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000) # use this line when using docker
    # app.run(debug=True)  # use this line when debugging the Flask app directluy
