import psycopg2
import matplotlib.pyplot as plt
from dotenv import load_dotenv
import os

def generate_report():
    # Load environment variables
    load_dotenv()

    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT')
        )

        # Query the top 10 pages with the most changes
        cur = conn.cursor()
        cur.execute("SELECT title, count FROM page_changes ORDER BY count DESC LIMIT 10")
        results = cur.fetchall()

        # Close the database connection
        cur.close()
        conn.close()

        # Prepare data for plotting
        titles = [row[0] for row in results]
        counts = [row[1] for row in results]

        # Create a bar plot
        plt.figure(figsize=(12, 6))
        plt.bar(range(len(titles)), counts)
        plt.title("Top 10 Wikipedia Pages with Most Changes")
        plt.xlabel("Page Title")
        plt.ylabel("Number of Changes")
        plt.xticks(range(len(titles)), titles, rotation=45, ha='right')
        plt.tight_layout()

        # Save the plot
        plt.savefig("wikipedia_changes_graph.png")
        plt.close()

        print("Graph saved as wikipedia_changes_graph.png")
    except Exception as e:
        print(f"Error generating report: {e}")

if __name__ == "__main__":
    generate_report()