from flask import Flask, request, send_file, render_template
import pandas as pd
import os

app = Flask(__name__)

@app.route('/')
def upload_form():
    return render_template('upload.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    file1 = request.files['file1']
    file2 = request.files['file2']
    
    # Read CSVs using pandas
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)
    
    # Concatenate DataFrames
    result_df = pd.concat([df1, df2], axis=0)

    # Save concatenated result to a file
    result_file = 'result.csv'
    result_df.to_csv(result_file, index=False)

    # Send file back to the user
    return send_file(result_file, as_attachment=True)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
