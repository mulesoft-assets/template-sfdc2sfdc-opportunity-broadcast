# Anypoint Template: SFDC to SFDC Opportunity Broadcast

+ [Use Case](#usecase)
+ [Run it!](#runit)
    * [Running on premise](#runonopremise)
    * [Running on CloudHub](#runoncloudhub)
    * [Properties to be configured](#propertiestobeconfigured)
+ [Customize It!](#customizeit)
    * [config.xml](#configxml)
    * [endpoints.xml](#endpointsxml)
    * [businessLogic.xml](#businesslogicxml)
    * [errorHandling.xml](#errorhandlingxml)
+ [Testing the Template](#testingthetemplate)
 

# Use Case <a name="usecase"/>
As a Salesforce admin I want to syncronize opportunities between two Salesfoce orgs.

This Template should serve as a foundation for setting an online sync of opportunities from one SalesForce instance to another. Everytime there is a new opportunity or a change in an already existing one, the integration will poll for changes in SalesForce source instance and it will be responsible for updating the opportunity on the target org.

Requirements have been set not only to be used as examples, but also to establish a starting point to adapt your integration to your requirements.

As implemented, this Template leverage the [Batch Module](http://www.mulesoft.org/documentation/display/current/Batch+Processing).
The batch job is divided in Input, Process and On Complete stages.
The integration is triggered by a poll defined in the flow that is going to trigger the application, querying newest SalesForce updates/creations matching a filter criteria and executing the batch job.
During the Process stage, each SFDC User will be filtered depending on, if it has an existing matching user in the SFDC Org B.
The last step of the Process stage will group the users and create/update them in SFDC Org B.
Finally during the On Complete stage the Template will logoutput statistics data into the console.

# Run it!

Simple steps to get SFDC to SFDC Opportunities Sync running.


In any of the ways you would like to run this Anypoint Template this is an example of the output you'll see after hitting the HTTP endpoint:

<pre>
<h1>Batch Process initiated</h1>
<b>ID:</b>6eea3cc6-7c96-11e3-9a65-55f9f3ae584e<br/>
<b>Records to Be Processed: </b>9<br/>
<b>Start execution on: </b>Mon Jan 13 18:05:33 GMT-03:00 2014
</pre>

## Running on premise <a name="runonopremise"/>

In this section we detail the way you have to run you Anypoint Temple on you computer.

### Running on Studio <a name="runonstudio"/>
Once you have imported your Anypoint Template into Anypoint Studio you need to follow these steps to run it:

+ Locate the properties file `mule.dev.properties`, in src/main/resources
+ Complete all the properties required as per the examples in the section [Properties to be configured](#propertiestobeconfigured)
+ Once that is done, right click on you Anypoint Template project folder 
+ Hover you mouse over `"Run as"`
+ Click on  `"Mule Application"`


### Running on Mule ESB stand alone <a name="runonmuleesbstandalone"/> 
Complete all properties in one of the property files, for example in [mule.prod.properties] (../blob/master/src/main/resources/mule.prod.properties) and run your app with the corresponding environment variable to use it. To follow the example, this will be `mule.env=prod`.

After this, to trigger the use case you just need to hit the local http endpoint with the port you configured in your file. If this is, for instance, `9090` then you should hit: `http://localhost:9090/migrateOpportunities` and this will create a CSV report and send it to the mails set.

## Running on CloudHub <a name="runoncloudhub"/>

While [creating your application on CloudHub](http://www.mulesoft.org/documentation/display/current/Hello+World+on+CloudHub) (Or you can do it later as a next step), you need to go to Deployment > Advanced to set all environment variables detailed in **Properties to be configured** as well as the **mule.env**. 

Once your app is all set and started, there is no need to do anything else. Every time a opportunity is created or modified, it will be automatically synchronised to SFDC Org B as long as it has an Email.

## Properties to be configured (With examples) <a name="propertiestobeconfigured"/>

In order to use this Template you need to configure properties (Credentials, configurations, etc.) either in properties file or in CloudHub as Environment Variables. Detail list with examples:

### Application configuration
+ http.port `9090` 
+ poll.frequencyMillis `60000`
+ poll.startDelayMillis `0`
+ watermark.defaultExpression `YESTERDAY`

#### Account Sync Policy
+ account.sync.policy `syncAccount`
+ account.id.in.b `001n0000003fMWXAA2`

**Note:** the property **account.sync.policy** can take any of the three following values: 

+ **empty_value**: if the propety has no value assigned to it then application will do nothing in what respect to the account and it'll just move the contact over.
+ **syncAccount**: it will try to create the contact's account should this is not pressent in the Salesforce instance B.
+ **assignDummyAccount**: it will assign the cotact to an pre existing account in Salesforce instance B. For this it will use the value of  `account.id.in.b`. Finding the Id of the desired Account can be done by executing in your **Sales Force Developer Console** the following query: `SELECT Id, Name, Description FROM Account`.


#### SalesForce Connector configuration for company A
+ sfdc.a.username `bob.dylan@orga`
+ sfdc.a.password `DylanPassword123`
+ sfdc.a.securityToken `avsfwCUl7apQs56Xq2AKi3X`
+ sfdc.a.url `https://login.salesforce.com/services/Soap/u/28.0`

#### SalesForce Connector configuration for company B
+ sfdc.b.username `joan.baez@orgb`
+ sfdc.b.password `JoanBaez456`
+ sfdc.b.securityToken `ces56arl7apQs56XTddf34X`
+ sfdc.b.url `https://login.salesforce.com/services/Soap/u/28.0`

# Customize It!<a name="customizeit"/>

This brief guide intends to give a high level idea of how this Template is built and how you can change it according to your needs.
As mule applications are based on XML files, this page will be organized by describing all the XML that conform the Template.
Of course more files will be found such as Test Classes and [Mule Application Files](http://www.mulesoft.org/documentation/display/current/Application+Format), but to keep it simple we will focus on the XMLs.

Here is a list of the main XML files you'll find in this application:

* [config.xml](#configxml)
* [inboundEndpoints.xml](#inboundendpointsxml)
* [businessLogic.xml](#businesslogicxml)
* [errorHandling.xml](#errorhandlingxml)


## config.xml<a name="configxml"/>
Configuration for Connectors and [Properties Place Holders](http://www.mulesoft.org/documentation/display/current/Configuring+Properties) are set in this file. **Even you can change the configuration here, all parameters that can be modified here are in properties file, and this is the recommended place to do it so.** Of course if you want to do core changes to the logic you will probably need to modify this file.

In the visual editor they can be found on the *Global Element* tab.


## businessLogic.xml<a name="businesslogicxml"/>
Functional aspect of the Template is implemented on this XML, directed by one flow that will poll for SalesForce creations/updates. The severeal message processors constitute four high level actions that fully implement the logic of this Template:

1. During the Input stage the Template will go to the SalesForce Org A and query all the existing users that match the filter criteria.
2. During the Process stage, each SFDC User will be filtered depending on, if it has an existing matching user in the SFDC Org B.
3. The last step of the Process stage will group the users and create/update them in SFDC Org B.
Finally during the On Complete stage the Template will logoutput statistics data into the console.

## endpoints.xml<a name="endpointsxml"/>
This is file is not used in this particular Template, but you'll oftenly find flows containing the inbound endpoints to start the integration.

## errorHandling.xml<a name="errorhandlingxml"/>
Contains a [Catch Exception Strategy](http://www.mulesoft.org/documentation/display/current/Catch+Exception+Strategy) that is only Logging the exception thrown (If so). As you imagine, this is the right place to handle how your integration will react depending on the different exceptions.

# Testing the Template <a name="testingthetemplate"/>

You will notice that the Template has been shipped with test.
These devidi them self into two categories:

+ Unit Tests
+ Integration Tests

You can run any of them by just doing right click on the class and clicking on run as Junit test.

Do bear in mind that you'll have to tell the test classes which property file to use.
For you convinience we have added a file mule.test.properties located in "src/test/resources".
In the run configurations of the test just make sure to add the following property:

+ -Dmule.env=test
