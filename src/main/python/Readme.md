#### Checkout data/a3 for a brief description   

### Dependencies

kafka-python  
nltk  
numpy   
pandas  
pyspark   
requests  

### Execution

<b>stream_producer.py - </b> hit the guardian api and dump the data into a given kafka topic. The file requires access_key, start_date and end_date to successfully fecth the data.    
<b>build_model.py - </b> create and train a model. This file requires the topic to be hardcoded. In the end, you will have a logistic regression model and a pipeline persisted in your current folder.    
<b>run_model.py - </b> fetch new data and check the performance of the model. The file requires access_key, start_date and end_date to successfully fetch the data.

### Commands

python stream_producer.py key_value start_date end_date   
python build_model.py   
python run_model.py key_value start_date end_date
