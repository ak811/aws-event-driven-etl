import boto3
import time
from flask import Flask

# --- CONFIGURATION - REPLACE THESE VALUES ---
AWS_REGION = "us-east-1"
ATHENA_DATABASE = "orders_db"
S3_OUTPUT_LOCATION = "s3://serverless-data-analytics-bucket/enriched/"
# -------------------------------------------

# Initialize Flask app and Boto3 client
app = Flask(__name__)
athena_client = boto3.client('athena', region_name=AWS_REGION)
s3_client = boto3.client('s3', region_name=AWS_REGION)

# List of your queries with descriptive titles
queries_to_run = [
    {
        "title": "1. Total Sales by Customer",
        "query": """
            SELECT Customer, SUM(Amount) AS TotalAmountSpent
            FROM "filtered_orders"
            GROUP BY Customer
            ORDER BY TotalAmountSpent DESC;
        """
    },
    {
        "title": "2. Monthly Order Volume and Revenue",
        "query": """
            SELECT DATE_TRUNC('month', CAST(OrderDate AS DATE)) AS OrderMonth,
            COUNT(OrderID) AS NumberOfOrders,
            ROUND(SUM(Amount), 2) AS MonthlyRevenue
            FROM "filtered_orders"
            GROUP BY 1 ORDER BY OrderMonth;
        """
    },
    {
        "title": "3. Order Status Dashboard",
        "query": """
            SELECT Status, COUNT(OrderID) AS OrderCount, ROUND(SUM(Amount), 2) AS TotalAmount
            FROM "filtered_orders"
            GROUP BY Status;
        """
    },
    {
        "title": "4. Average Order Value (AOV) per Customer",
        "query": """
            SELECT Customer, ROUND(AVG(Amount), 2) AS AverageOrderValue
            FROM "filtered_orders"
            GROUP BY Customer
            ORDER BY AverageOrderValue DESC;
        """
    },
    {
        "title": "5. Top 10 Largest Orders in February 2025",
        "query": """
            SELECT OrderDate, OrderID, Customer, Amount
            FROM "filtered_orders"
            WHERE CAST(OrderDate AS DATE) BETWEEN DATE '2025-02-01' AND DATE '2025-02-28'
            ORDER BY Amount DESC LIMIT 10;
        """
    }
]

def run_athena_query(query):
    """Starts an Athena query and waits for it to complete."""
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': ATHENA_DATABASE},
            ResultConfiguration={'OutputLocation': S3_OUTPUT_LOCATION}
        )
        query_execution_id = response['QueryExecutionId']

        # Poll for query completion
        while True:
            stats = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = stats['QueryExecution']['Status']['State']
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(1) # Wait 1 second before checking again

        if status == 'SUCCEEDED':
            s3_path = stats['QueryExecution']['ResultConfiguration']['OutputLocation']
            bucket_name, key = s3_path.replace("s3://", "").split("/", 1)

            # Fetch results from S3
            s3_response = s3_client.get_object(Bucket=bucket_name, Key=key)
            lines = s3_response['Body'].read().decode('utf-8').splitlines()

            # Parse CSV into a list of lists
            header = [h.strip('"') for h in lines[0].split(',')]
            results = [[val.strip('"') for val in line.split(',')] for line in lines[1:]]
            return header, results
        else:
            error_message = stats['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
            return None, f"Query failed: {error_message}"
    except Exception as e:
        return None, f"An exception occurred: {str(e)}"

@app.route('/')
def index():
    """Main route to display the dashboard."""
    html_content = "<html><head><title>Athena Orders Dashboard</title>"
    html_content += """
        <style>
            body { font-family: sans-serif; margin: 2em; background-color: #f4f4f9; }
            h1 { color: #333; }
            h2 { color: #555; border-bottom: 2px solid #ddd; padding-bottom: 5px; }
            table { border-collapse: collapse; width: 80%; margin-top: 20px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }
            th, td { border: 1px solid #ccc; padding: 10px; text-align: left; }
            th { background-color: #007bff; color: white; }
            tr:nth-child(even) { background-color: #f2f2f2; }
        </style>
    </head><body>"""
    html_content += "<h1>📊 Athena Orders Dashboard</h1>"

    for item in queries_to_run:
        html_content += f"<h2>{item['title']}</h2>"

        header, results = run_athena_query(item['query'])

        if header and results is not None:
            html_content += "<table><thead><tr>"
            for col in header:
                html_content += f"<th>{col}</th>"
            html_content += "</tr></thead><tbody>"
            for row in results:
                html_content += "<tr>"
                for cell in row:
                    html_content += f"<td>{cell}</td>"
                html_content += "</tr>"
            html_content += "</tbody></table>"
        else: # Handle errors
            html_content += f"<p style='color:red;'><strong>Error:</strong> {results}</p>"

    html_content += "</body></html>"
    return html_content

if __name__ == '__main__':
    # Run the app on all available network interfaces
    app.run(host='0.0.0.0', port=5000)

