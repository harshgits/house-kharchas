from flask import Flask, request, render_template

app = Flask(__name__)


@app.route("/", methods=["GET", "POST"])
def input_form():
    ownership_table_new = None  # Initialize the result as None
    if request.method == "POST":
        # Get the input values from the form
        undocd_kharchas = request.form["undocd_kharchas"]
        ownership_table = request.form["ownership_table"]

        # Concatenate the input values
        ownership_table_new = undocd_kharchas + ownership_table

    # Render the template, pass the concatenated result to the template
    return render_template(
        "input_with_result.html", ownership_table_new=ownership_table_new
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
