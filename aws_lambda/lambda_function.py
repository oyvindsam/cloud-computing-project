import os
import io
import urllib.parse

import boto3
from matplotlib import pyplot as plt
import smart_open
from wordcloud import WordCloud


print('Loading function')

s3 = boto3.resource('s3')


def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    sort_path = os.path.dirname(key)
    email_path = os.path.dirname(sort_path)
    partfile_path = sort_path + '/' + 'part-r-00000'
    try:
        figure = generate_wordcloud('s3://' + bucket + '/' + partfile_path)
        s3 = boto3.client('s3')
        s3.upload_fileobj(figure, bucket, email_path + "/image.png", ExtraArgs={'ACL': 'public-read'})
    except Exception as e:
        raise e


def generate_wordcloud(partfile_path):
    d = {}
    stopwords = {}
    for line in smart_open.open('s3://s3801950-wordcount/input/stopwords.txt'):
        stopwords[line.strip()] = 1

    maxlines = 30
    for line in smart_open.open(partfile_path):
        word, count = line.split()
        if word.lower() not in stopwords:
            d[word] = float(count)
            maxlines -= 1
            if maxlines < 0: break

    wordcloud = WordCloud()
    wordcloud.generate_from_frequencies(frequencies=d)
    plt.figure()
    plt.imshow(wordcloud, interpolation="bilinear")
    plt.axis("off")
    in_mem_file = io.BytesIO()
    plt.savefig(in_mem_file, format='png')
    in_mem_file.seek(0)
    return in_mem_file


