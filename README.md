# GoogleBigQueryUpload

This program uploads all the n-gram files from Google Cloud Storage (which you can upload through Google Cloud Tool). It will import
each n-gram file into its equivalent Bigquery table. The table format is as follow:

column1 column2
word count

We can infer the month from the table name.
