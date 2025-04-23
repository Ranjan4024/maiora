from flask import Flask, jsonify
import requests
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///jokes.db'
db = SQLAlchemy(app)


class Joke(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    category = db.Column(db.String(50))
    type = db.Column(db.String(50))
    joke = db.Column(db.String(500))
    setup = db.Column(db.String(500))
    delivery = db.Column(db.String(500))
    nsfw = db.Column(db.Boolean)
    political = db.Column(db.Boolean)
    sexist = db.Column(db.Boolean)
    safe = db.Column(db.Boolean)
    lang = db.Column(db.String(10))


@app.route('/fetch_jokes', methods=['GET'])
def fetch_jokes():
    response = requests.get('https://v2.jokeapi.dev/joke/Any?amount=6')
    jokes = response.json()['jokes']

    for joke_data in jokes:
        joke = Joke(
            category=joke_data['category'],
            type=joke_data['type'],
            joke=joke_data.get('joke'),
            setup=joke_data.get('setup'),
            delivery=joke_data.get('delivery'),
            nsfw=joke_data['flags']['nsfw'],
            political=joke_data['flags']['political'],
            sexist=joke_data['flags']['sexist'],
            safe=joke_data['flags']['safe'],
            lang=joke_data['lang']
        )
        db.session.add(joke)
    db.session.commit()

    return jsonify({'message': 'Jokes fetched and stored successfully!'})


if __name__ == '__main__':
    db.create_all()
    app.run(debug=True)
