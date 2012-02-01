package spot

import scala.actors._
import scala.actors.Actor._
import scala.collection.immutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.ec2.AmazonEC2
import com.amazonaws.services.ec2.AmazonEC2Client
import com.amazonaws.auth.PropertiesCredentials
import com.amazonaws.services.ec2.model.DescribeSpotInstanceRequestsRequest
import com.amazonaws.services.ec2.model.DescribeInstancesRequest
import com.amazonaws.services.ec2.model.Filter
import com.amazonaws.services.ec2.model.RequestSpotInstancesRequest
import com.amazonaws.services.ec2.model.LaunchSpecification
import com.amazonaws.services.ec2.model.EbsBlockDevice
import com.amazonaws.services.ec2.model.RequestSpotInstancesResult
import com.amazonaws.services.ec2.model.Instance
import com.amazonaws.services.ec2.model.DescribeInstancesResult
import com.amazonaws.services.ec2.model.Reservation
import com.amazonaws.services.ec2.model.SpotInstanceRequest

import java.util.ArrayList


//@author Juan Carlos Rhenals
//Spot Instance Manager v.01
object AWSSpotInstanceManager {
    def main(args: Array[String]) {
 
    	UserInterface.start()
    }
}

case class Display(context:String)
case class Request(context:String)


//main class
object UserInterface  {

	def start() { 
	  //begin querying every 60 secs
	  queryInstanceController ! Start(60000)
	  
	  //send a few test calls
	  theDirector ! Request("spotInstanceRequest") 
	  theDirector ! Display("viewInstances") 
	  theDirector ! Display("viewRequests")
	
	}
 

	val myGuiActor = actor{
	  def act(){
	    loop{
	      receive{
	        case msg => print("cough")
	      }
	    }
	  }
	}
	val ec2 = initializeEC2()
	val theDirector = new TheDirector(ec2)
	val queryInstanceController = new QueryInstancesController(ec2)
	val inputController = new InputController
	//val requestActor = new RequestActor

	  
	def initializeEC2(): AmazonEC2 = {
		//supply credentials
		val credentials = getAWSCredentials()
		return new AmazonEC2Client(credentials)
	}
	def getAWSCredentials() : PropertiesCredentials = {
		// Retrieves the credentials from an AWSCredentials.properties file.
		val credentials  = new PropertiesCredentials(
					this.getClass().getResourceAsStream("AwsCredentials.properties"))
		//check exception handling here
		return credentials;
		
	}
		
	
	

}


//Display Data functions
class DisplayActor(amz:AmazonEC2) extends Actor{
  
  val ec2 = amz
  def act() {
    loop{
      react{
        case "viewRequests" => actor{
          println("Lions and Tigers and Bears, Oh Shit!")
          //make call to get instances
          val res = ec2.describeSpotInstanceRequests( new DescribeSpotInstanceRequestsRequest())    
          //Ideally we can send this to some display actor
          println(getSpotInstanceRequestDataString( res.getSpotInstanceRequests() ))
        }
        case "viewInstances" => actor{
          //make call to get instances          
          val res = ec2.describeInstances(new DescribeInstancesRequest()
				.withFilters( new Filter("instance-lifecycle")
					.withValues("spot")) )
		  //Ideally we can send this to some display actor
		  println(getSpotInstancesDataString(res.getReservations()))
          
        }
        case "viewUptimeAndCost" => actor{
          //make call to get uptime and cost
          
        }
        case "viewPricing" => actor{
          //make call to get Pricing (needs parameters)

        }
      }
    }
  }
  start()
  def getSpotInstanceRequestDataString( listOfInstances: java.util.List[SpotInstanceRequest]):String = {
    var messageToDisplay = ""
    if(listOfInstances.isEmpty())
	    messageToDisplay += "\nThere are no current requests."
    else{
    
	  messageToDisplay += "\n***********************************************"
	  messageToDisplay += "\n**********AWS Spot Instance Requests***********"
	  messageToDisplay += "\n***********************************************"
	  messageToDisplay += "\n\tRequest ID\tState\tType\tSpot Price"
	  var count = 0
	  for( spReq <- listOfInstances ){
		messageToDisplay += ("\n" + count + "\t " + spReq.getSpotInstanceRequestId() + "\t" 
				+ spReq.getState() + "\t" + spReq.getType() + "\t" 
				+ spReq.getSpotPrice())
		count += 1
	}
   }
   return messageToDisplay
  }
  def getSpotInstancesDataString( listOfReservations: java.util.List[Reservation] ) :String = {
	  var messageToDisplay = ""
      if(listOfReservations.isEmpty())
        messageToDisplay += "\nThere are no instances running."
      else{
   		messageToDisplay += "\n***********************************************"
		messageToDisplay += "\n**********AWS Spot Instances*******************"
		messageToDisplay += "\n***********************************************"
		messageToDisplay += "\nid\tRequest ID\tInstance ID\tState\tReason"
		var count = 0
        for( eachOne <- listOfReservations ){
          for( instance <- eachOne.getInstances() ){
            messageToDisplay += ("\n" + count + "\t" + instance.getSpotInstanceRequestId() + "\t" + instance.getInstanceId() + "\t" 
					+ instance.getState().getName() + "\t" +  instance.getStateTransitionReason())
			count += 1
          }
        }
      }
	  return messageToDisplay
  }
}


//Requests to do things
case class AttachEBS()
case class AttachElasticIP()
case class TerminateInstance()
case class TerminateRequest()
case class CreateRequest()
case class launchSSH()
class RequestActor(amz:AmazonEC2) extends Actor{
  
  val ec2 = amz
  
  def act(){
    loop{
	  react{
	    case CreateRequest => actor{
	      //just default action right now to create Spot Request with set parameters
	      val requestResult = ec2.requestSpotInstances( createSpotRequest() );        	
	      val requestResponses = requestResult.getSpotInstanceRequests();
    	
	      for ( requestResponse <- requestResponses) {
	    	 println("Created Spot Request: "+requestResponse.getSpotInstanceRequestId())
	      }     
	    }
	    case AttachEBS() =>
	    case AttachElasticIP() =>
	    case _ => println("Dont know what to do!")
	  }
    }

  }
  start()
  def createSpotRequest() : RequestSpotInstancesRequest = {
		
		
		// Initializes a Spot Instance Request
    	val requestRequest = new RequestSpotInstancesRequest();
   
    	// Request 1 x t1.micro instance with a bid price of $0.03. 
    	requestRequest.setSpotPrice("0.05");
    	requestRequest.setInstanceCount(Integer.valueOf(3));
    	requestRequest.setAvailabilityZoneGroup("us-east-1d");
    	
    	// Setup the specifications of the launch. This includes the instance type (e.g. t1.micro)
    	// and the latest Amazon Linux AMI id available. Note, you should always use the latest 
    	// Amazon Linux AMI id or another of your choosing.
    	val launchSpecification = new LaunchSpecification();
    	launchSpecification.setImageId("ami-8c1fece5");
    	launchSpecification.setInstanceType("t1.micro");

    	// Add the security group to the request.
    	val securityGroups = new ArrayList[String]()
    	securityGroups.add("default");
    	launchSpecification.setSecurityGroups(securityGroups); 

    	// Add the launch specifications to the request.
    	requestRequest.setLaunchSpecification(launchSpecification);	
	
		return requestRequest;
		
	}
}

//the actor who is the initial delegator
class TheDirector(amz:AmazonEC2) extends Actor{
  val ec2 = amz
  val displayActor = new DisplayActor(ec2)
  val requestActor = new RequestActor(ec2)
  def act() { 
      loop {
        react {
          case Display(context) =>
            displayActor ! context
          case Request(context) =>
            requestActor ! context
          case msg =>
            println("Unhandled message: " + msg)
        }
      }
    }
    start()
}

//Singleton that holds the PostOffice and sends 'envelopes' to it
object PostOfficeDirectory extends Actor{
  val postOffice = new PostOffice
  def act() { 
    loop {
      react {
        case envelope =>
          postOffice ! envelope
      }
    }
  }
  start()
}

//the PostOffice
class PostOffice extends Actor{
def act() { 
      loop {
        react {
          
          case ("changeState",b:String,c:String,d:String,e:String) =>
            println("postOffice received changeState message") 
            println( b + c + d + e)
            UserInterface.myGuiActor ! "postOffice received changeState message"
          case ("newInstance",b:String,c:String,d:String,e:String) =>
            println("postOffice received newInstance message") 
            println( b + c + d + e)
            UserInterface.myGuiActor ! "postOffice received newInstance message"
          case _ => println("Don't know what to do!")
          	UserInterface.myGuiActor ! "postOffice received I dont know message"
            
          
        }
      }
      
    }
  	start()  
}


case class Start(long:Long)
case class Poll(mapOfInstancesFromLastPoll:HashMap[String,Instance])
case class StartPoll
//the controller that will run as its own actor, checking the status of the 
//  instances that are running and sending a message to PostOfficeDirectory
//  when a change is noted
class QueryInstancesController(amz:AmazonEC2) extends Actor{
  
  val ec2 = amz

  var pollTime = 60001L // arbitrary default

  def act() {   
    loop {
        react {
          
          case Start(long) =>
          	val qic = self
            actor{ 
          		this.pollTime = long  
          		qic ! StartPoll
          	}
          case StartPoll =>
	     	val qic = self
            actor{qic ! Poll(queryInstances())}  
          case Poll(mapOfInstancesFromLastPoll) =>
	     	val qic = self
            actor{
	     		//this actually returns (mapOfInstancesFromLastPoll , listOfEnvelopes)
		     	val mapOfInstancesFromLastPollANDlistOfEnvelopesTuple
		     		= compareInstances(mapOfInstancesFromLastPoll)//return a list of messages

		     	for( envelope <- mapOfInstancesFromLastPollANDlistOfEnvelopesTuple._2 )
		     	  actor{PostOfficeDirectory ! envelope}
		     	  //fix this referenece to postOffice -> causing NoClassDefFound Exception
		     	  
		     	println("about to Sleep for " + this.pollTime)
		     	Thread.sleep(pollTime)
		     	qic ! Poll(mapOfInstancesFromLastPollANDlistOfEnvelopesTuple._1)
	    
	     	}
        }
      }
      
    }
  	start()

  	def compareInstances(mapOfInstancesFromLastPoll:HashMap[String,Instance]) = {
  	  val res = ec2.describeInstances(new DescribeInstancesRequest()
				.withFilters( new Filter("instance-lifecycle")
					.withValues("spot")) )
  	  
  	  
  	  //iterate through result set
  	  //check if instance ids from results are in the prevData hashmap

  	  //this hashmap will replace the prevData when we are done
	  val tempData = new collection.mutable.HashMap[String,Instance]
  	  var envelopeList = new ListBuffer[(String,String,String,String,String)]()

  	  //do comparison
  	  for(  rsv <- res.getReservations()){
			for(instance <- rsv.getInstances() ){
			  tempData.put(instance.getInstanceId(), instance)
			  val oldInstance = mapOfInstancesFromLastPoll.get(instance.getInstanceId())
			  
			  //handy tuple we can use to store all the string values we will pass to the 'postOffice'
			  //Wild cards in functions are pretty cool to me.
			  val envelope = (_:String, _:String, instance.getState().getName(),instance.getInstanceId(), 
				      if(instance.getSpotInstanceRequestId() == null) "" else instance.getSpotInstanceRequestId() )
			  
			  oldInstance match {
			    case Some(oldInstance) => 
			      if( !oldInstance.getState().getName().equals(instance.getState().getName()) )
			    	   envelopeList += envelope("changeState", oldInstance.getState().getName())
			    //if it wasnt there before, it must be new
			    case None =>  envelopeList += envelope("newInstance", "") 
			  }
			}
  	  }
  	  (HashMap(tempData.toSeq: _*), envelopeList.toList)
  	}
  	def queryInstances() : HashMap[String,Instance] = {
  	  val res = ec2.describeInstances(new DescribeInstancesRequest()
				.withFilters( new Filter("instance-lifecycle")
					.withValues("spot")) )
  	  return queryInstances(res:DescribeInstancesResult)
  	}
  	
  	def queryInstances(res:DescribeInstancesResult) : HashMap[String,Instance] = {
  	  //first thing we want to do is take record of the current Spot Requests that exist
		//DescribeSpotInstanceRequestsRequest req = new DescribeSpotInstanceRequestsRequest()
			/*.withFilters(new Filter("state")
				.withValues("active")
				.withValues("open")
				.withValues("closed")
				.withValues("failed"))*/;
			
		//initialize our hashmap of ids and their states
		var instances = new collection.mutable.HashMap[String,Instance]
		for(  rsv <- res.getReservations()){
			for(instance <- rsv.getInstances() ){
			  instances.put(instance.getInstanceId(), instance);
			}
		}
		return HashMap(instances.toSeq: _*)
		
  	}
     
  }

class InputController extends Actor{
  def act() { 
      loop {
        react {
          
          case _ => println("WTF am I doing?? I don't even belong in this program. I don't do shit!")  
            
          
        }
      }
      
    }
  	start()
}