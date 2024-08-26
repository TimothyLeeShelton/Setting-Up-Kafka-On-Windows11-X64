import psycopg2
import matplotlib.pyplot as plt

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname="wikipedia_changes",
    user="your_username",
    password="your_password",
    host="localhost",
    port="5432"
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
plt.bar(titles, counts)
plt.title("Top 10 Wikipedia Pages with Most Changes")
plt.xlabel("Page Title")
plt.ylabel("Number of Changes")
plt.xticks(rotation=45, ha='right')
plt.tight_layout()

# Save the plot
plt.savefig("wikipedia_changes_graph.png")
plt.close()

print("Graph saved as wikipedia_changes_graph.png")