## About dags..

**upload_data** --> this the first dag in pipeline,we created this dag to extract data from the file ( which serves as sensors ) , and the upload the extracted data to the database                     ,awaiting processing with following dag . 
                
**get_data** --> this the second dag in pipeline, This dag retrieves the raw data uploaded to database ,processes it,feeds it int the model for prediction , and then uploads the 
                 processed data to the database. 


**train_model** --> this final dag in pipeline , this dag retrieves the processed data from database and retrain the model on this data then save it . 
