### Results

Approximately 15000 articles were taken from a given two year period. 80 % of these were used to train our logistic regression model and the rest was for validation. Accuracy attained on this validation set was approximately 70 %. This model was then persisted.

The persisted model was loaded in order to run it on streaming data. The accuracy achieved was approximately 66.10 %.

### Logistic Regression

Number of iterations: 100
Alpha parameter: 0.2
Accuracy: 65%

### Random Forest Classifier

Number of trees: 40
Max depth: 4
Accuracy: 21%

### Note:

Initial accuracy of 40 % was improved to 70 % by increasing the training data set from 4000 articles to 15000 articles. Therefore, further increasing the dataset would surely help in creating a better trained model.

In case of Random Forest Classifier, due to memory limitations I could only attain an accuracy of 21% for 7000 articles.