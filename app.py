from expenses_table_tools import ExpensesTableTools
from flask import Flask, request, render_template, send_file
import io

app = Flask(__name__)

@app.route("/", methods=["GET", "POST"])
def input_form():
    if request.method == "POST":
        # Get the uploaded file
        uploaded_file = request.files.get("file")
        if uploaded_file and uploaded_file.filename.endswith(".csv"):
            # Read the file content
            file_content = uploaded_file.stream.read().decode("utf-8")

            # update the to-date columns in the expenses csv_string
            file_content_new = ExpensesTableTools.update_todate_cols_in_expenses_table(file_content)
            
            # Create a CSV response
            output = io.StringIO()
            output.write(file_content_new)
            output.seek(0)
            return send_file(
                io.BytesIO(output.getvalue().encode("utf-8")),
                mimetype="text/csv",
                as_attachment=True,
                download_name="expenses_with_ownership.csv"
            )
    return render_template("webpage.html")

if __name__ == "__main__":
    # app.run(host="0.0.0.0", port=5000) # use this line when using docker
    app.run(debug=True)  # use this line when debugging the Flask app directly
