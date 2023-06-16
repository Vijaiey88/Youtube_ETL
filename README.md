# YouTube ETL and Visualize

This is a Streamlit app that allows users to enter a YouTube channel ID, retrieve channel details using the YouTube API, store the data in a MongoDB data lake, migrate it to a SQL data warehouse, and query the warehouse to display the data in the app.

Usage
Run the app (In terminal)
streamlit run youtube_etl_dashboard.py
Access the app in your web browser at http://localhost:8501.

Enter a YouTube channel ID in the input field and click the "Retrieve Channel Details" button.

The app will retrieve the channel details from the YouTube API, store the data in MongoDB, migrate it to the SQL data warehouse, and display the retrieved data in the app.

You can use the provided filters or search functionality in the app to query the SQL data warehouse and view specific channel data.

Contributions are welcome! If you have any suggestions, bug reports, or feature requests, please open an issue or submit a pull request.
