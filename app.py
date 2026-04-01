from flask import Flask, render_template, request
import pandas as pd
import io

app = Flask(__name__)

def reconcile_data(internal_df, bank_df):
    # Ensure column names are stripped of whitespace
    internal_df.columns = internal_df.columns.str.strip()
    bank_df.columns = bank_df.columns.str.strip()

    report = {
        "duplicates": [],
        "missing_in_bank": [],
        "mismatches": [],
        "orphans": []
    }

    # 1. Detect Duplicates in Bank Records 
    dupes = bank_df[bank_df.duplicated(subset=['txn_id'], keep=False)]
    report["duplicates"] = dupes.to_dict(orient='records')

    # Merge for comparison
    merged = pd.merge(internal_df, bank_df, left_on='transaction_id', right_on='txn_id', how='outer')

    # 2. Missing in Bank 
    missing = merged[merged['txn_id'].isna()]
    report["missing_in_bank"] = missing[['transaction_id', 'amount', 'date']].to_dict(orient='records')

    # 3. Rounding/Amount Mismatches 
    mismatch = merged[merged['txn_id'].notna() & merged['transaction_id'].notna()]
    mismatch = mismatch[mismatch['amount'] != mismatch['amt']]
    report["mismatches"] = mismatch[['transaction_id', 'amount', 'amt']].to_dict(orient='records')

    # 4. Orphans (Refunds/Entries with no match) 
    orphans = merged[merged['transaction_id'].isna()]
    report["orphans"] = orphans[['txn_id', 'amt', 'clear_date']].to_dict(orient='records')

    return report

@app.route('/', methods=['GET', 'POST'])
def upload_files():
    if request.method == 'POST':
        file1 = request.files['internal_csv']
        file2 = request.files['bank_csv']
        
        if file1 and file2:
            internal_df = pd.read_csv(io.StringIO(file1.stream.read().decode("UTF8")))
            bank_df = pd.read_csv(io.StringIO(file2.stream.read().decode("UTF8")))
            
            results = reconcile_data(internal_df, bank_df)
            return render_template('report.html', results=results)
            
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)