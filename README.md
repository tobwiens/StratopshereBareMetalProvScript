stratDeployScriptEc2
==============

Running the script - Installs and configures ONLY Stratosphere 
-------------------
1. Write configuration file
2. run {python StartStratosphereInstance.py}


Configuration file
-------------------
- conf/instances.cfg - configuration file

Create an empty configuration file:
------------------------------------
* create conf/ folder
* create an empty conf/instances.cfg


Sample configuration file:
---------------------------
* [Basic]
* key-ID = {Your ID}
* aws-secret-key = {Your secret}
* region = eu-west-1
* key-path = {Key created in aws and saved locally in pem file}
* key-name = {Name of the key}
* ip-access = 0.0.0.0/0
    
* [Instance]
* username = ubuntu
* image-ID = ami-808675f7
* instance-type = m1.small
* security-group = whirr-provisioning-instance #This group has to be created

* [Java]
* java-home-directory = /opt/java
