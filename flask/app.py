from flask import Flask, render_template, request, send_file
import io

app = Flask(__name__)

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/process_text', methods=['POST'])
def process_text():
    text = request.json['text']

    from sqlalchemy import create_engine
    import pandas as pd

    user = 'root'
    passwd = 'root'
    host = '192.168.219.123'
    db = 'stock'

    engine = create_engine(f'postgresql://{user}:{passwd}@{host}/{db}')
    engine.connect()

    query = f'SELECT * FROM predict_count WHERE code = \'{text}\';'
    
    df=pd.read_sql(query, con=engine)

    import matplotlib.pyplot as plt
    import numpy as np

    # Get the count values for each category
    count_0 = df['0.0'].values
    count_1 = df['1.0'].values
    count_2 = df['2.0'].values

    # Create an array of x-coordinates for each date
    x = np.arange(len(df))

    # Plot the stacked histogram
    plt.figure(figsize=(12, 6))
    bar_width = 0.5
    opacity = 0.8

    plt.bar(x, count_2, bar_width, alpha=opacity, label='Prediction Neatural')
    plt.bar(x, count_1, bar_width, bottom=count_2, alpha=opacity, label='Prediction Sell')
    plt.bar(x, count_0, bar_width, bottom=np.add(count_1, count_2), alpha=opacity, label='Prediction Buy')

    # Add labels to each histogram
    for i in range(len(x)):
        total_count = count_0[i] + count_1[i] + count_2[i]
        plt.text(x[i], total_count, f"Buy" if count_0[i] > count_1[i]  else
                                    f"Sell" if count_1[i] > count_0[i] else
                                    f"Neutral", ha='center', va='bottom')

    # Customize the plot
    plt.xlabel('Date')
    plt.ylabel('Count')
    plt.title(f'Stock Code : {text} Buy/Sell/Neutral')
    plt.xticks(x, df['date'], rotation=45)
    plt.legend()

    # Save the plot as an image
    image_stream = io.BytesIO()
    plt.savefig(image_stream, format='png')
    image_stream.seek(0)

    return send_file(image_stream, mimetype='image/png')

if __name__ == '__main__':
    app.run(host='0.0.0.0')