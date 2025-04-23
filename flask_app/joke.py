from flask import Flask, g, jsonify 
import requests
import sqlite3

app = Flask(__name__)
DATABASE = 'jokes.db'


def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
    return db


@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()


def init_db():
    with app.app_context():
        db = get_db()
        with app.open_resource('../database/jokes.sql', mode='r') as f:
            db.cursor().executescript(f.read())
        db.commit()


# Create the database and tables if they don't exist.  
# You'll need to define the schema.sql file.
try:
    init_db()
except sqlite3.OperationalError as e:
    print(f"Database initialization failed: {e}")
    # Handle this error appropriately, like exiting the application or logging it.


@app.route('/fetch_jokes', methods=['GET'])
def fetch_jokes():
    response = requests.get('https://v2.jokeapi.dev/joke/Any?amount=16')
    if response.status_code != 200:  # Check for a successful API request
        return jsonify({'message': 'Failed to fetch jokes from the API'}), response.status_code

    jokes = response.json().get('jokes', [])  # Handle case where 'jokes' might be missing

    db = get_db()
    cursor = db.cursor()

    for joke_data in jokes:
        try:  # Use try-except to handle potential errors during insertion
            cursor.execute('''
                INSERT INTO jokes (category, type, joke, setup, delivery, nsfw, political, sexist, safe, lang)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                joke_data.get('category'),
                joke_data.get('type'),
                joke_data.get('joke'),
                joke_data.get('setup'),
                joke_data.get('delivery'),
                joke_data.get('flags', {}).get('nsfw'),  # Safely access nested dicts
                joke_data.get('flags', {}).get('political'),
                joke_data.get('flags', {}).get('sexist'),
                joke_data.get('flags', {}).get('safe'),
                joke_data.get('lang')
            ))
        except sqlite3.Error as e:
            print(f"Error inserting joke: {e}")  # Log the error
            db.rollback()  # Rollback the transaction if any error occurs
            return jsonify({'message': 'Error storing jokes'}), 500
    db.commit()  # Commit after all jokes are inserted successfully
    cursor.execute("select * from jokes")
    return jsonify({'message': f'Jokes fetched and stored successfully! {str(cursor.fetchall())}'})


if __name__ == '__main__':
    app.run(debug=True)