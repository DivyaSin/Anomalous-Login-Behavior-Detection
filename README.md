# Anomalous-Login-Behavior-Detection
Detect anomalous failed login behavior. Given an audit log with multiple types of records, we are interested only in those records of type USER_LOGIN (the rest you should discard). The output of the program should be the list of users whose anomaly score is higher than the threshold specified by the user at the command line.

Here is a sample failed USER_LOGIN record:

type=USER_LOGIN msg=audit(1453738391.690:107584): user pid=23159 uid=0 auid=4294967295 ses=4294967295 msg='op=login acct="jcasaletto" exe="/usr/sbin/sshd" hostname=? addr=10.20.30.200 terminal=ssh res=failed' 
NOTE: this is a failed login attempt by a user called "jcasaletto"

Here is a sample successful USER_LOGIN record:

type=USER_LOGIN msg=audit(1456937762.214:56406): user pid=6548 uid=0 auid=496 ses=5648 msg='op=login id=496 exe="/usr/sbin/sshd" hostname=172.30.3.169 addr=172.30.3.169 terminal=ssh res=success'

NOTE: this is a successful login attempt by a user with id=496. Also note that the successful USER_LOGIN record contains an id in the msg, NOT an acct.

MapReduce Program 1

The first MapReduce program (MRdriver.java, MRmapper1.java, and MRreducer1.java) will calculate the following statistic:
* failed_login_attempts_for_acct
Note: acct must be replaced by each user name as defined in the "acct" field of the USER_LOGIN record. In other words, there will be multiple output lines for the failed_login_attempts_for_acctstatistic (one per user name with failed logins).
 
Hints:

1. It's always a great idea to write your mapper first to make sure that your intermediate results are what you expect. When you have "good" output from your mapper class, proceed to implementing the reducer.
2. You must run a single reducer to ensure your statistics are calculated over all the data.
3. You must use the MRmapper1.java, MRreducer1.java, and MRdriver.java provided by the instructor.

MapReduce Program 2 

The second MapReduce program (MRdriver.java, MRmapper2.java, and MRreducer2.java)  will calculate the following statistics:
* mean_failed_login_attempts
* sigma_failed_login_attempts
* num_sigmas_for:acct
 
Note: acct must be replaced by each user name as defined in the "acct" field of the USER_LOGIN record. In other words, there will be multiple output lines for the num_sigmas_for:acct statistic (one per user name with failed logins).
 
The standard deviation (denoted as "sigma") is the square root of the variance, and the variance is denoted as "sigma squared". Use the formula below to calculate the standard deviation:
 
sigma = sqrt ( 1/N * [ (x1 - u)^2 +  (x2 - u)^2 + ... + (xN - u)^2 ] )
