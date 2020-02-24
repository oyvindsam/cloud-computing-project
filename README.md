# Fast Distributed Word Cloud

This highly distributed cloud application generates a word cloud for a user’s uploaded file. The website runs on Google Cloud’s webapp2 framework for Python 2.7. The file is then uploaded to AWS’ S3, which triggers the lambda function to start EMR clusters and subsequently the MapReduce job for counting the words. The result is saved to S3, then a second lambda function is triggered to create the word cloud. In the end, the result can be retrieved from the website.

Event graph
![](https://github.com/oyvindsam/cloud-computing-project/blob/master/graph.jpg?raw=true)
