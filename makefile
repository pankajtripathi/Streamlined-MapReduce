# Used Only to create jar if the user want a new jar
# As we are using JSCH to run scripts, we do not need makefile for other purposes
# Please refer to Readme.txt for more details

buildjar:
	cd MRFramework && mvn clean && mvn package 
	mv MRFramework/target/MRFramework-0.0.1-SNAPSHOT-jar-with-dependencies.jar node.jar


# add jar to your local maven repository 
addmaven:
	mvn install:install-file -Dfile=node.jar -DgroupId=com.google.code -DartifactId=mrframework -Dversion=1.0.0 -Dpackaging=jar
