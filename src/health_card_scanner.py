import boto3
import pandas as pd
import time
import io
from io import BytesIO
import sys
import logging
from botocore.exceptions import ClientError
import re
import math
from datetime import datetime
from PIL import Image, ImageDraw, ImageFont

class HealthCardScanner():

	def __init__(self):
		self.access_key_id  = "S3_ACCESS_KEY_ID"
		self.secret_access_key= "S3_SECRET_ACCESS_KEY"
		self.s3_client = boto3.client('s3',
      region_name="us-east-2",
      aws_access_key_id=self.access_key_id,
      aws_secret_access_key=self.secret_access_key)
		self.s3_resource = boto3.resource('s3',
			region_name="us-east-2",
			aws_access_key_id=self.access_key_id,
			aws_secret_access_key=self.secret_access_key)
		self.textract_client = boto3.client('textract',
			region_name="us-east-2",
			aws_access_key_id=self.access_key_id,
			aws_secret_access_key=self.secret_access_key)

	def upload_file(self, file_name, bucket, object_name=None):
		"""Upload a file to an S3 bucket
			:param file_name: File to upload
    	:param bucket: Bucket to upload to
    	:param object_name: S3 object name. If not specified then file_name is used
    	:return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
		if object_name is None:
			object_name = file_name

    # Upload the file
		try:
			response = self.s3_client.upload_file(file_name, bucket, object_name)
		except ClientError as e:
			logging.error(e)
			return False
		return True

  # Filter word blocks with confidence level > 70%
	def filter_top_words(self,blocks):
		print("Filtering top words...........")
		top_words = []
		for block in blocks:
			if 'Confidence' in block:
				if(block['Confidence']> 70 and block['Text'] not in top_words): # Remove repeated words
					top_words.append(block['Text'])
		return top_words

	def get_text_analysis(self,bucket,document):
		# Get the document from S3
		print("Importing image..........")
		s3_object = self.s3_resource.Object(bucket,document)
		s3_response = s3_object.get()
		stream = io.BytesIO(s3_response['Body'].read())
		image=Image.open(stream)
    
		image_binary = stream.getvalue()
		print('Extracting words..........')
		response = self.textract_client.analyze_document(Document={'Bytes': image_binary},FeatureTypes=["TABLES", "FORMS"])
      
    # Get the text blocks
		blocks=response['Blocks']
		print ('Detected Document Text')

		# Filter words with confidence level > 70%
		top_words = self.filter_top_words(blocks)
		return top_words
	
	# Concatenate OHIP digits and characters
	def get_ohip_string(self,nums,chars):
		remove_chars = ['-',' ']
		for char in remove_chars:
			chars = chars.replace(char,'')
			nums = nums.replace(char,'')
		return(nums+chars)

	# Parse resulting Textract string for OHIP card fields
	def get_ohip(self,text):
		name = re.compile('(?<=[Health] Sante )(.*?)(?= \d)').findall(text)[0]
		ohip_num = re.compile("(\d{4}(( |-){1,3}\d{3})( |-){1,3}\d{3})").findall(text)[0][0]
		ohip_chars = re.compile("[\D]{2}(?= BORN)").findall(text)[0]
		ohip = self.get_ohip_string(ohip_num,ohip_chars)
		birth_year = int(re.compile("(?<=BORN\/NE\(E )\d{4}").findall(text)[0])
		birth_month = int(re.compile("(?<=BORN\/NE\(E \d{4}[ -])\d{2}").findall(text)[0])
		birth_day = int(re.compile("(?<=BORN\/NE\(E \d{4}[ -]\d{2}[ -])\d{2}").findall(text)[0])
		birthdate = datetime(birth_year,birth_month,birth_day)
		issue_year = int(re.compile("(?<=EXP\/EXP )\d{4}").findall(text)[0])
		issue_month = int(re.compile("(?<=EXP\/EXP \d{4}-)\d{2}").findall(text)[0])
		issue_day = int(re.compile("(?<=EXP\/EXP \d{4}-\d{2}[ -]{3})\d{2}").findall(text)[0])
		issuedate = datetime(issue_year,issue_month,issue_day)
		expdate = datetime(issue_year+5,birth_month,birth_day)
		ohip_dict={
			"name": name,
			"ohip": ohip,
			"birthdate": birthdate,
			"issuedate": issuedate,
			"expdate": expdate
		}
		return(ohip_dict)


	def main(self,bucket,document):
		block_count=self.get_text_analysis(bucket,document)
		seperator = " "
		card_scanned = seperator.join(block_count)
		ohip_dict = self.get_ohip(str(card_scanned))
		return(ohip_dict)

scanner = HealthCardScanner()
# scanner.upload_file("../images/health_card.jpg",'s3-bucket-name')
result = scanner.main("s3-bucket-name","health_card.jpg")
print(result)