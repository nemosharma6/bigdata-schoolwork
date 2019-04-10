import requests
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import RandomForestClassificationModel
# import sys


def get_data(u):
    json_data = requests.get(u).json()
    d = []

    for ind in range(len(json_data["response"]['results'])):
        headline = json_data["response"]['results'][ind]['fields']['headline']
        body_text = json_data["response"]['results'][ind]['fields']['bodyText']
        headline += ". "
        headline += body_text
        label = json_data["response"]['results'][ind]['sectionName']
        temp = list()
        temp.append(label)
        temp.append(headline)
        d.append(temp)

    return d


if __name__ == "__main__":

    sc = SparkContext()
    sqlContext = SQLContext(sc)

    random_forest_model = RandomForestClassificationModel.load("rf.model")

    if len(sys.argv) != 4:
        print('Number of arguments is not correct')
        exit()
    
    key = sys.argv[1]
    from_date = sys.argv[2]
    to_date = sys.argv[3]

    url = 'http://content.guardianapis.com/search?from-date=' + from_date + '&to-date=' + to_date + \
          '&order-by=newest&show-fields=all&page-size=200&%20num_per_section=10000&api-key=' + key

    data = get_data(url)
    df = sqlContext.createDataFrame(data, schema=["category", "text"])
    pipeline_fit = PipelineModel.load("rf_pipeline")
    data_set = pipeline_fit.transform(df)

    predictions = random_forest_model.transform(data_set)

    evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
    percent = evaluator.evaluate(predictions)
    print(percent * 100)
