package warehouse;
import java.sql.*;
import java.util.*;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.bag.SynchronizedSortedBag;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.time.LocalDate;
import java.util.Calendar;
import java.util.Locale;
public class MeshJoin 
{

	public static void main(String[] args)
	{
//main
		Scanner input=new Scanner(System.in);
		System.out.println("Enter Your Username to connect with MYSQL: ");
		String username=input.next();
		System.out.println("Enter Password: ");
		String password=input.next();
		System.out.println("Enter Schema Name: ");
		String schema = input.next();
		//---------------------------------------
		//ASK THE USER TO INPUT USERNAME & PASSWORD TO CONNECT WITH "MYSQL"
		//MY USER: root	pswd:1234 schema_name:metro
		try
		{
			
			//--------------------------------------
			Connection mycon = DriverManager.getConnection("jdbc:mysql://localhost:3306/"+schema, username, password);
			System.out.println("CONNECTED TO DATABASE SUCCESSFULLY");
			System.out.println("Now Implementing MeshJoin...Please wait");
			while(true)
			{
				int md_partition=0;
				//---------------------------------------
				int number_of_partitions=10;
				//---------------------------------------
				ArrayBlockingQueue<List<List<String>>> que = new ArrayBlockingQueue<List<List<String>>>(number_of_partitions);//10 capacity/partitions
				//---------------------------------------		
				int total_rows=10000+450;//TOTAL NUMBER OF ROWS TO BE READ IN TRANSACTION TABLE
				//---------------------------------------
				//HASHTABLE  <KEY(PRODUCT_ID): VALUE(WHOLE RECORD)>
				MultiValuedMap<String, List> hashmap = new ArrayListValuedHashMap<String, List>();
				//---------------------------------------
				Statement query0 = mycon.createStatement();
				String md_query0 = "select count(*) from FACTS";//QUERY
				PreparedStatement q0 = mycon.prepareStatement(md_query0);
				ResultSet r0 = q0.executeQuery();
				r0.next();
				String check = r0.getString("count(*)");
				if (check.equals("10000"))
				{
					System.out.println("ALL DATA READ FROM TRANSACTION TABLE...\nNOW BREAKING FROM LOOP");
					break;
				}
			//---------------------------------------
				for (int iter2=0;iter2<total_rows;iter2=iter2+50)//TRAVERSING THROUGH TRANSACTIONS TABLE
				{
					//-------------------------------------
					//READING FROM TRANSACTIONS TABLE
					Statement query = mycon.createStatement();
					String md_query = "select * from transactions limit "+ iter2 + ",50";//QUERY
					PreparedStatement q = mycon.prepareStatement(md_query);
					ResultSet r = q.executeQuery();
					//-------------------------------------
					List <String> s = new ArrayList<String>();//LIST FOR ss
					List <List<String>> ss = new ArrayList<List<String>>();//LIST FOR QUEUE
					ss.clear();
					s.clear();
					//-------------------------------------
					while(r.next()) //TRAVERSING THE RESULT
					{
						//---------------------------------------------					
						s.add(r.getString("TRANSACTION_ID"));
						s.add(r.getString("PRODUCT_ID"));
						ss.clear();
						ss.add(s);
						//---------------------------------------------
						//FOLLOWING WILL BE ADDED TO THE HASH TABLE
						// <PRODUCT_ID: <TUPLE RECORD>>
						List <String> s1 = new ArrayList<String>();
						s1.add(r.getString("TRANSACTION_ID"));
						s1.add(r.getString("PRODUCT_ID"));
						s1.add(r.getString("CUSTOMER_ID"));
						s1.add(r.getString("CUSTOMER_NAME"));
						s1.add(r.getString("STORE_ID"));
						s1.add(r.getString("STORE_NAME"));
						s1.add(r.getString("T_DATE"));
						s1.add(r.getString("QUANTITY"));
						//ADDING IN HASHTABLE
						hashmap.put(r.getString("PRODUCT_ID"),s1);
	
						//----------------------------------------------
					}
					//--------------------------------
					//ADDING TRANSACTION_IDS IN QUEUE
					if (que.remainingCapacity()>0) 
					{
						que.offer(ss);
					}
					else
					{
						//----------------------------
						//REMOVING THE HEAD FROM THE QUEUE
						List<String> tempp = new ArrayList<String>();
						tempp= que.take().get(0);//DEQUEUE()

						for (int ii=0;ii<100;ii=ii+2)
						{
							List<List> rem = new ArrayList<List>( hashmap.get(tempp.get(ii+1)));
							for (int j=0;j<rem.size();j++)
							{
								if (rem.get(j).get(0).equals(tempp.get(ii))) 
								{
	//								System.out.print("\nMatched : "+tempp.get(ii)+"-->"+rem.get(j)+"\n");
	//								System.out.println("\nRemoving "+tempp.get(ii+1)+" matched tuple : "+ rem.get(j));
									try {
										Statement ins = mycon.createStatement();
										String ins_query = "INSERT INTO CUSTOMERS(CUSTOMER_ID,CUSTOMER_NAME) VALUES('"+rem.get(j).get(2)+"','"+rem.get(j).get(3)+"')" ;//QUERY
										PreparedStatement q1 = mycon.prepareStatement(ins_query);
										q1.execute();
									}
									catch(Exception e){}
									try 
									{
										Statement ins = mycon.createStatement();
										String ins_query = "INSERT INTO STORES(STORE_ID,STORE_NAME) VALUES('"+rem.get(j).get(4)+"','"+rem.get(j).get(5)+"')" ;;
										PreparedStatement q1 = mycon.prepareStatement(ins_query);
										q1.execute();
									}
									catch(Exception e){}
									try 
									{
										Statement ins = mycon.createStatement();
										String ins_query = "INSERT INTO DATES(DATE_ID) VALUES('"+rem.get(j).get(6)+"')" ;;
										PreparedStatement q1 = mycon.prepareStatement(ins_query);
										q1.execute();
									}
									catch(Exception e){}
									hashmap.removeMapping(tempp.get(ii+1),rem.get(j));
	//								break;
								}
							}
						}					
					}
	
					//-------------------------------
					//READING MASTER DATA
					
					Statement query1 = mycon.createStatement();
					String md_query1 = "select * from MASTERDATA limit "+ md_partition*number_of_partitions + ","+number_of_partitions;//QUERY
					PreparedStatement q1 = mycon.prepareStatement(md_query1);
					ResultSet r1 = q1.executeQuery();
					//--------------------------------
					//TRAVERSING THROUGH CURRENT PARTITION OF MASTER_DATA
					while(r1.next())
					{
						String match = (r1.getString("PRODUCT_ID"));
						try {//POPULATING DIMENSIONS
							Statement ins = mycon.createStatement();
							String ins_query = "INSERT INTO PRODUCTS(PRODUCT_ID,PRODUCT_NAME,PRICE) VALUES('"+r1.getString("PRODUCT_ID")+"','"+r1.getString("PRODUCT_NAME")+"'," + r1.getString("PRICE")+")";
							PreparedStatement q11 = mycon.prepareStatement(ins_query);
							q11.execute();
						}
						catch(Exception e){
						}
						try 
						{
							Statement ins = mycon.createStatement();
							String ins_query = "INSERT INTO SUPPLIERS(SUPPLIER_ID,SUPPLIER_NAME) VALUES('"+r1.getString("SUPPLIER_ID")+"','"+r1.getString("SUPPLIER_NAME")+"')";
							PreparedStatement q11 = mycon.prepareStatement(ins_query);
							q11.execute();
						}
						catch(Exception e){}
						try {
							
							Statement ins = mycon.createStatement();
							String ins_query="Insert into SUPPLIERS (SUPPLIER_ID,SUPPLIER_NAME) values ('SP-15','Casey''s General Stores Inc.')";
							PreparedStatement q11 = mycon.prepareStatement(ins_query);
							q11.execute();
						}
						catch(Exception e){}
						//-------------------------------------
						//FINDING IN HASHTABLE
						if (hashmap.get(match).size()>0)
						{
							
							List<List> rem = new ArrayList<List>( hashmap.get(match));
							for (int i=0;i<hashmap.get(match).size();i++)
							{
								Float sale;
								Float price;
								price = Float.parseFloat(r1.getString("PRICE")) ;
								sale = Float.parseFloat((String) rem.get(i).get(7) ) * price;
	//							System.out.println(rem.get(i).get(7)+"*"+price +": "+sale+" Sale");
								String ins_query = "INSERT INTO FACTS(TRANSACTION_ID,DATE_ID,PRODUCT_ID,STORE_ID,SUPPLIER_ID,CUSTOMER_ID,SALE) VALUES('"+rem.get(i).get(0)+"','"+rem.get(i).get(6)+"','"+match+"','"+rem.get(i).get(4)+"','"+r1.getString("SUPPLIER_ID")+"','"+rem.get(i).get(2)+"',"+sale+")";
	
								try {
									Statement ins = mycon.createStatement();
									PreparedStatement q11 = mycon.prepareStatement(ins_query);
									q11.execute();
								}
								catch(Exception e)
								{
									try 
									{
										Statement ins = mycon.createStatement();
										PreparedStatement q11 = mycon.prepareStatement(ins_query);
										q11.execute();
									}
									catch (Exception ee){}
								}
	
							}
						}
						else
						{
						}
						//-------------------------------------
					}
					if(md_partition<9) //CHANGING THE COUNTER OF CURRENT PARTITION OF MASTER_DATA
					{
						md_partition=md_partition+1;
					}
					else	//RESETTING IT TO FIRST PARTITION
					{
						md_partition=0;
					}
					//-------------------------------
	
				}
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

}
