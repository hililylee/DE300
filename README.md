n terminal: cd .ssh → cd id_rsa

2) Start Instance on AWS

3) Connect Instance by clicking Connect → SSH Client

4) Type the following into terminal but replace the ubuntu link with the new link: 
ssh -i "DE300_INS1.pem" -L 8888:localhost:8888  ubuntu@ec2-3-149-235-138.us-east-2.compute.amazonaws.com 

5) Finger print (yes/no) select yes

6) In terminal: tmux

7) In your terminal, run the command aws configure sso --no-browser and enter the following values:
 
SSO session name: nu-sso
SSO region: us-east-2
CLI default client Region [None]: us-east-2
CLI default output format [None]: text
CLI profile name [mse-tl-dataeng300-EMR-549787090008]: de300lily

Also, type in your export information:

export AWS_ACCESS_KEY_ID=""
export AWS_SECRET_ACCESS_KEY=""
export AWS_SESSION_TOKEN=""
export AWS_DEFAULT_REGION=us-east-2

I found that region is important to input after the keys!

8) In terminal: cd DE300 → cd Homework1 → cd src

9) In terminal: jupyter notebook --ip=0.0.0.0

10) Copy and paste the link provided into browser window to open jupyter notebook
If the jupyter notebook not found: pip install --upgrade --user jupyter
If the jupyter notebook doesn’t run: bash run.sh

11) Replace the s3 boto3 client credentials with your export access key id, secret access key, and session token

12) Run each cell in the jupyter notebook

13) To view the updated database, download HeartDisease.db and open in sqlite

