This parser is used to extract content (dataset for model training) from WARC files . for those who doesn't know WARC files this are the one which contains the HTML content which is scraped from the internet and store in Different format like WARC, WET.

for using this parser first install the requirment from requirement.txt file

```bash
pip install -r requirement.txt
```

and then for running these parser you needed WARC files you can able to download it from common crawl site or if you have AWS account then you can download it from s3 itself.
after that run the below given command 

```bash
python3 parser.py "/path/input_folder/" -o "/path/output_folder/" --workers nproc 
```
define the both input and output path , for faster completion use more workers. 

yeahh that's it ..
