#connection is established with mongodb and output file is open
filename='IP1.txt'
from pymongo import MongoClient
client = MongoClient('localhost:27017')
db = client.database1
db.transactiontable.drop()
db.locktable.drop()

o = open('outputfile', 'w')
timestamp=0

#doit function reads all the lines of inputfile and does so iteratively until all the transactions have commited
def doit():
	global timestamp
	f = open(filename, 'r')
	#this for loop reads all the lines of inputfile one-time 
	for x in f: 
		#for each instruction read timestamp is increased by one
		timestamp=timestamp+1
		#to bypass firstlin of inputfile
		if x[0]=='I':
			timestamp=timestamp-1
		#if begin transaction instruction is encountered,before spawning a new transaction first it is checked whether
		#that transaction already exists(in active,waiting,aborted mode) or not
		if x[0]=='b':
			count1=db.transactiontable.find( { 'tid': x[1],'transstate' :'active' } ).count()
			count2=db.transactiontable.find( { 'tid': x[1],'transstate' :'committed' } ).count()
			count3=db.transactiontable.find( { 'tid': x[1],'transstate' :'aborted' } ).count()
			count5=db.transactiontable.find( { 'tid': x[1],'transstate' :'waiting' } ).count()
			if count1==0 and count2==0 and count3==0 and count5==0: 
				db.transactiontable.insert({"tid" : x[1],"timestamp" : timestamp,"transstate":'active',"itemshold":''})
		
		#read statement is encountered
		if x[0]=='r':
			cur = db.transactiontable.find({'tid': x[1]}, {'transstate':1, '_id':0})
			st= str(cur[0]).split("'")
			transstate=str(st[3])
			#it does something only if transaction is active
			if transstate=='active':
			
				if x[3]=='(':
					dataitem=x[4]
				else:
					dataitem=x[3]
				count1=db.locktable.find( { 'itemname': dataitem} ).count()
				#if dataitem is encountered for the first,thenit is inserted in lock table with its 
				#appropriate status(read,write) and transaction table is updated to show the dataitem in tid's held resources
				if count1==0:
					db.locktable.insert({'itemname':dataitem,'lockstate':'read','lockholdingtid':'-'+x[1],'lockwaitingtid':''})
					cur = db.transactiontable.find({'tid': x[1]}, {'itemshold':1, '_id':0})
					st= str(cur[0]).split("'")
					itemshold=str(st[3])
					db.transactiontable.update_one({"tid": x[1]},{"$set": {"itemshold": itemshold+'-'+dataitem}    })
				
				#syntax correction code
				cur = db.locktable.find({ 'itemname': dataitem}, {'lockholdingtid':1, '_id':0})
				st= str(cur[0]).split("'")
				locktid=str(st[3])
				if locktid=='':
					db.locktable.update_one({'itemname':dataitem},{"$set":{'lockholdingtid':'-'+x[1]}  })
					cur = db.transactiontable.find({'tid': x[1]}, {'itemshold':1, '_id':0})
					st= str(cur[0]).split("'")
					itemshold=str(st[3])
					db.transactiontable.update_one({"tid": x[1]},{"$set": {"itemshold": itemshold+'-'+dataitem}    })
				else:
					#loop for handling dataitem being previously held by some tid
					if count1>0:
						cur = db.locktable.find({'itemname': dataitem}, {'lockstate':1, '_id':0})
						st= str(cur[0]).split("'")
						state = st[3]
						#if it is readlocked,then the requesting tid is also added to the lockhelding tids
						#and locktable and transactiontable accordingly updated
						if state=='read':
							cur = db.locktable.find({'itemname': dataitem}, {'lockholdingtid':1, '_id':0})
							st= str(cur[0]).split("'")
							lockholdingtid = str(st[3]) + '-' + str(x[1])
							db.locktable.update_one({"itemname": dataitem},{"$set": {"lockholdingtid": lockholdingtid}    })
							cur = db.transactiontable.find({'tid': x[1]}, {'itemshold':1, '_id':0})
							st= str(cur[0]).split("'")
							itemshold=str(st[3])
							db.transactiontable.update_one({"tid": x[1]},{"$set": {"itemshold": itemshold+'-'+dataitem}    })
						#if dataitem is write locked
						if state=='write':
							#first tids of lock requesting and lock holding transactions is fetched 
							wantingtid=x[1]
							cur = db.transactiontable.find({'tid': wantingtid}, {'timestamp':1, '_id':0})
							st= str(cur[0]).split(":")
							c=str(st[1])
						
							if c[2]=='}':
								wantingtidtimestamp=c[1]
							
							else:
								wantingtidtimestamp=c[1:3]
							
						
						
							cur = db.locktable.find({'itemname': dataitem}, {'lockholdingtid':1, '_id':0})
							st= str(cur[0]).split("'")
							holdingtid = str(st[3]) 
							cur = db.transactiontable.find({'tid': holdingtid[1]}, {'timestamp':1, '_id':0})
							st= str(cur[0]).split(":")
							c=str(st[1])
							if c[2]=='}':
								holdingtidtimestamp=c[1]
							else:
								holdingtidtimestamp=c[1:3]
					
							#if both are equal(llike in case of lock upgradation or degradation)
							#lock is provided,and lock and transtable updated
							if int(wantingtidtimestamp)==int(holdingtidtimestamp):
								db.locktable.update_one({"itemname": dataitem},{"$set": {"lockstate": 'read'}    })
							#if lock-holding tid is older than requesting tid
							#then inlock table-requesting tid is added in lockwaitingtids column with status
							#requesting,example -r3
							if int(wantingtidtimestamp)>int(holdingtidtimestamp):
							
								cur = db.locktable.find({'itemname': dataitem}, {'lockwaitingtid':1, '_id':0})
								st= str(cur[0]).split("'")
								lockwaitingtid = str(st[3]) + '-' + 'r'+str(x[1])
								db.locktable.update_one({"itemname": dataitem},{"$set": {"lockwaitingtid": lockwaitingtid}    })
							
							#if lock-holding tid is younger than requesting tid,than it is aborted, resource it helds released,
							#the particular dataitem provided to requesting tid
							#and other dataitems provided to waiting tids(for read) and to one tid (if write lock) 
							#All the changes updated in both the tables
								
							if int(wantingtidtimestamp)<int(holdingtidtimestamp):
								db.locktable.update_one({"itemname": dataitem},{"$set": {"lockholdingtid": '-'+wantingtid,'lockstate':'read'}    })
								cur = db.transactiontable.find({'tid': wantingtid}, {'itemshold':1, '_id':0})
								st= str(cur[0]).split("'")
								itemshold=str(st[3])
								db.transactiontable.update_one({"tid": wantingtid},{"$set": {"itemshold": itemshold+'-'+dataitem}    })
							
								db.transactiontable.update_one({"tid": holdingtid[1]},{"$set": {"transstate": 'aborted'}    })
								cur = db.transactiontable.find({'tid': holdingtid[1]}, {'itemshold':1, '_id':0})
								st= str(cur[0]).split("'")
								itemshold=str(st[3])
								#condition for distributing aborted trans resources other than requested by requesting transaction
								for i in range(0,len(itemshold)):
									if itemshold[i]!='-' and itemshold[i]!=dataitem:
										cur = db.locktable.find({'itemname': itemshold[i]}, {'lockstate':1, '_id':0})
										st= str(cur[0]).split("'")
										state = st[3]
										cur = db.locktable.find({'itemname': itemshold[i]}, {'lockholdingtid':1, '_id':0})
										st= str(cur[0]).split("'")
										lockholdingtid = st[3]
										#if the released resource is write-locked by the aborted trans
										if state=='write':
											cur = db.locktable.find({'itemname': itemshold[i]}, {'lockwaitingtid':1, '_id':0})
											st= str(cur[0]).split("'")
											lockwaitingtid = str(st[3]) 
											#check if there is any tid waiting or not,if present grants the first transaction
											#in the queue the lock and do all updation
											if len(lockwaitingtid)>0:
												newstate=lockwaitingtid[1]
												if newstate=='r':
													newstate='read'
												if newstate=='w':
													newstate='write'	
												newlockholdingtransaction=lockwaitingtid[2]
												newlockwaitingtransactions=lockwaitingtid[3:]
												db.locktable.update_one({"itemname": itemshold[i]},{"$set": {"lockholdingtid": newlockholdingtransaction,'lockstate': newstate,"lockwaitingtid":newlockwaitingtransactions}    })
												cur = db.transactiontable.find({"tid": lockwaitingtid[2]}, {'itemshold':1, '_id':0})
												st= str(cur[0]).split("'")
												tempo = str(st[3]) 
												db.transactiontable.update_one({"tid": lockwaitingtid[2]},{"$set": {"transstate": 'active',"itemshold":tempo+'-'+itemshold[i]}    })
										
										#first checks whether the resource is readlocked by other resource or not,if
										#locked,only delete the tid of aborting transaction.if not readlocked by any other tid, 
										#check if there is any tid waiting or not,if present grants the first transaction
										#in the queue the lock and do all updation of both the tables
										if state=='read':
											if len(lockholdingtid)>1:
												z=lockholdingtid
												for k in range(0,len(z)):
													if z[k]==holdingtid:
														str1=z[0:k-1]
														str2=z[k+1:]
												db.locktable.update_one({"itemname": itemshold[i]},{"$set": {"lockholdingtid": str1+str2}    })	
											else:
												cur = db.locktable.find({'itemname': itemshold[i]}, {'lockwaitingtid':1, '_id':0})
												st= str(cur[0]).split("'")
												lockwaitingtid = str(st[3]) 
												if len(lockwaitingtid)>0:
													newstate=lockwaitingtid[1]
													if newstate=='r':
														newstate='read'
													if newstate=='w':
														newstate='write'	
													newlockholdingtransaction=lockwaitingtid[2]
													newlockwaitingtransactions=lockwaitingtid[3:]
													db.locktable.update_one({"itemname": itemshold[i]},{"$set": {"lockholdingtid": newlockholdingtransaction,'lockstate': newstate,"lockwaitingtid":newlockwaitingtransactions}    })
													cur = db.transactiontable.find({"tid": lockwaitingtid[2]}, {'itemshold':1, '_id':0})
													st= str(cur[0]).split("'")
													tempo = str(st[3]) 
													db.transactiontable.update_one({"tid": lockwaitingtid[2]},{"$set": {"transstate": 'active',"itemshold":tempo+'-'+itemshold[i]}    })
										
							#################
		#condition for writing instructions,executes only if transactive
		if x[0]=='w':
			cur = db.transactiontable.find({'tid': x[1]}, {'transstate':1, '_id':0})
			st= str(cur[0]).split("'")
			transstate=str(st[3])
			if transstate=='active':
		
		
				if x[3]=='(':
					dataitem=x[4]
				else:
					dataitem=x[3]
				count1=db.locktable.find( { 'itemname': dataitem} ).count()
				#if dataitem is encountered for the first,thenit is inserted in lock table with its 
				#appropriate status(read,write) and transaction table is updated to show the dataitem in tid's held resources
				if count1==0:
					db.locktable.insert({'itemname':dataitem,'lockstate':'write','lockholdingtid':'-'+x[1],'lockwaitingtid':''})
					cur = db.transactiontable.find({'tid': x[1]}, {'itemshold':1, '_id':0})
					st= str(cur[0]).split("'")
					itemshold=str(st[3])
					db.transactiontable.update_one({"tid": x[1]},{"$set": {"itemshold": itemshold+'-'+dataitem}    })
				#syntax correction code
				cur = db.locktable.find({ 'itemname': dataitem}, {'lockholdingtid':1, '_id':0})
				st= str(cur[0]).split("'")
				locktid=str(st[3])
				if locktid=='':
					db.locktable.update_one({'itemname':dataitem},{"$set":{'lockholdingtid':'-'+x[1]}  })
					cur = db.transactiontable.find({'tid': x[1]}, {'itemshold':1, '_id':0})
					st= str(cur[0]).split("'")
					itemshold=str(st[3])
					db.transactiontable.update_one({"tid": x[1]},{"$set": {"itemshold": itemshold+'-'+dataitem}    })
				else:
					#loop for handling dataitem being previously held by some tid
					if count1>0:
						#first tids of lock requesting and lock holding transactions is fetched 
						wantingtid=x[1]
						cur = db.transactiontable.find({'tid': wantingtid}, {'timestamp':1, '_id':0})
						st= str(cur[0]).split(":")
						c=str(st[1])
						if c[2]=='}':
								wantingtidtimestamp=c[1]
						else:
							wantingtidtimestamp=c[1:3]
						
						cur = db.locktable.find({'itemname': dataitem}, {'lockholdingtid':1, '_id':0})
						st= str(cur[0]).split("'")
						holdingtid = str(st[3]) 
						
						cur = db.transactiontable.find({'tid': holdingtid[1]}, {'timestamp':1, '_id':0})
						st= str(cur[0]).split(":")
						
						c=str(st[1])
						if c[2]=='}':
							holdingtidtimestamp=c[1]
						else:
							holdingtidtimestamp=c[1:3]
						#if both are equal(llike in case of lock upgradation or degradation)
						#lock is provided,and lock and transtable updated
						if int(wantingtidtimestamp)==int(holdingtidtimestamp):
							db.locktable.update_one({"itemname": dataitem},{"$set": {"lockstate": 'write'}    })
						#if lock-holding tid is older than requesting tid
						#then inlock table-requesting tid is added in lockwaitingtids column with status
						#requesting,example -w3
						if int(wantingtidtimestamp)>int(holdingtidtimestamp):
							cur = db.locktable.find({'itemname': dataitem}, {'lockwaitingtid':1, '_id':0})
							st= str(cur[0]).split("'")
							lockwaitingtid = str(st[3]) + '-' + 'w'+str(x[1])
							db.locktable.update_one({"itemname": dataitem},{"$set": {"lockwaitingtid": lockwaitingtid}    })
						
						#if lock-holding tid is younger than requesting tid,than it is aborted, resource it helds released,
						#the particular dataitem provided to requesting tid
						#and other dataitems provided to waiting tids(for read) and to one tid (if write lock) 
						#All the changes updated in both the tables	
						if int(wantingtidtimestamp)<int(holdingtidtimestamp):
							db.locktable.update_one({"itemname": dataitem},{"$set": {"lockholdingtid": '-'+wantingtid,"lockstate":'write'}    })
							
							cur = db.transactiontable.find({'tid': wantingtid}, {'itemshold':1, '_id':0})
							st= str(cur[0]).split("'")
							itemshold=str(st[3])
							db.transactiontable.update_one({"tid": wantingtid},{"$set": {"itemshold": itemshold+'-'+dataitem}    })
							
							db.transactiontable.update_one({"tid": holdingtid[1]},{"$set": {"transstate": 'aborted'}    })
							cur = db.transactiontable.find({'tid': holdingtid[1]}, {'itemshold':1, '_id':0})
							st= str(cur[0]).split("'")
							itemshold=str(st[3])
							######
							#condition for distributing aborted trans resources other than requested by requesting transaction
							for i in range(0,len(itemshold)):
									if itemshold[i]!='-' and itemshold[i]!=dataitem:
										cur = db.locktable.find({'itemname': itemshold[i]}, {'lockstate':1, '_id':0})
										st= str(cur[0]).split("'")
										state = st[3]
										cur = db.locktable.find({'itemname': itemshold[i]}, {'lockholdingtid':1, '_id':0})
										st= str(cur[0]).split("'")
										lockholdingtid = st[3]
										#if the released resource is write-locked by the aborted trans
										if state=='write':
											cur = db.locktable.find({'itemname': itemshold[i]}, {'lockwaitingtid':1, '_id':0})
											st= str(cur[0]).split("'")
											lockwaitingtid = str(st[3]) 
											#check if there is any tid waiting or not,if present grants the first transaction
											#in the queue the lock and do all updation
											if len(lockwaitingtid)>0:
												newstate=lockwaitingtid[1]
												if newstate=='r':
													newstate='read'
												if newstate=='w':
													newstate='write'	
												newlockholdingtransaction=lockwaitingtid[2]
												newlockwaitingtransactions=lockwaitingtid[3:]
												db.locktable.update_one({"itemname": itemshold[i]},{"$set": {"lockholdingtid": newlockholdingtransaction,'lockstate': newstate,"lockwaitingtid":newlockwaitingtransactions}    })
												cur = db.transactiontable.find({"tid": lockwaitingtid[2]}, {'itemshold':1, '_id':0})
												st= str(cur[0]).split("'")
												tempo = str(st[3]) 
												db.transactiontable.update_one({"tid": lockwaitingtid[2]},{"$set": {"transstate": 'active',"itemshold":tempo+'-'+itemshold[i]}    })
										#first checks whether the resource is readlocked by other resource or not,if
										#locked,only delete the tid of aborting transaction.if not readlocked by any other tid, 
										#check if there is any tid waiting or not,if present grants the first transaction
										#in the queue the lock and do all updation of both the tables
										if state=='read':
											if len(lockholdingtid)>1:
												z=lockholdingtid
												for k in range(0,len(z)):
													if z[k]==holdingtid:
														str1=z[0:k-1]
														str2=z[k+1:]
												db.locktable.update_one({"itemname": itemshold[i]},{"$set": {"lockholdingtid": str1+str2}    })	
											else:
												cur = db.locktable.find({'itemname': itemshold[i]}, {'lockwaitingtid':1, '_id':0})
												st= str(cur[0]).split("'")
												lockwaitingtid = str(st[3]) 
												if len(lockwaitingtid)>0:
													newstate=lockwaitingtid[1]
													if newstate=='r':
														newstate='read'
													if newstate=='w':
														newstate='write'	
													newlockholdingtransaction=lockwaitingtid[2]
													newlockwaitingtransactions=lockwaitingtid[3:]
													db.locktable.update_one({"itemname": itemshold[i]},{"$set": {"lockholdingtid": newlockholdingtransaction,'lockstate': newstate,"lockwaitingtid":newlockwaitingtransactions}    })
													cur = db.transactiontable.find({"tid": lockwaitingtid[2]}, {'itemshold':1, '_id':0})
													st= str(cur[0]).split("'")
													tempo = str(st[3]) 
													db.transactiontable.update_one({"tid": lockwaitingtid[2]},{"$set": {"transstate": 'active',"itemshold":tempo+'-'+itemshold[i]}    })
										
		#if commiting instruction is encountered,proceedes only if transaction is active
		if x[0]=='c':
			cur = db.transactiontable.find({'tid': x[1]}, {'transstate':1, '_id':0})
			st= str(cur[0]).split("'")
			transstate=str(st[3])
			if transstate=='active':
			
				count4=db.transactiontable.find( { 'tid': x[1],'transstate' :'active' } ).count()
				if count4>0:
					
					cur = db.transactiontable.find({'tid': x[1]}, {'itemshold':1, '_id':0})
					st= str(cur[0]).split("'")
					itemshold=str(st[3])
					db.transactiontable.update_one({"tid": x[1]},{"$set": {"transstate": 'committed',"itemshold":''}    })
					#releases al the locks held as,same code as done previously by aborted transactions(in read and write instructions)
					#transaction and lock table updated accordingly
					for i in range(0,len(itemshold)):
								if itemshold[i]!='-' :
									cur = db.locktable.find({'itemname': itemshold[i]}, {'lockstate':1, '_id':0})
									st= str(cur[0]).split("'")
									state = st[3]
									cur = db.locktable.find({'itemname': itemshold[i]}, {'lockholdingtid':1, '_id':0})
									st= str(cur[0]).split("'")
									lockholdingtid = st[3]
									if state=='write':
										cur = db.locktable.find({'itemname': itemshold[i]}, {'lockwaitingtid':1, '_id':0})
										st= str(cur[0]).split("'")
										lockwaitingtid = str(st[3]) 
										if len(lockwaitingtid)>0:
											newstate=lockwaitingtid[1]
											if newstate=='r':
												newstate='read'
											if newstate=='w':
												newstate='write'	
											newlockholdingtransaction='-'+lockwaitingtid[2]
											newlockwaitingtransactions=lockwaitingtid[3:]
											db.locktable.update_one({"itemname": itemshold[i]},{"$set": {"lockholdingtid": newlockholdingtransaction,'lockstate': newstate,"lockwaitingtid":newlockwaitingtransactions}    })
											cur = db.transactiontable.find({"tid": lockwaitingtid[2]}, {'itemshold':1, '_id':0})
											st= str(cur[0]).split("'")
											tempo = str(st[3]) 
											db.transactiontable.update_one({"tid": lockwaitingtid[2]},{"$set": {"transstate": 'active',"itemshold":tempo+'-'+itemshold[i]}    })
										else:
											db.locktable.update_one({"itemname": itemshold[i]},{"$set": {"lockholdingtid": '','lockstate': ''}    })
									if state=='read':
										if len(lockholdingtid)>1:
											z=lockholdingtid
											for k in range(0,len(z)):
												if z[k]==x[1]:
													str1=z[0:k-1]
													str2=z[k+1:]
											db.locktable.update_one({"itemname": itemshold[i]},{"$set": {"lockholdingtid": str1+str2}    })	
											
										else:
											cur = db.locktable.find({'itemname': itemshold[i]}, {'lockwaitingtid':1, '_id':0})
											st= str(cur[0]).split("'")
											lockwaitingtid = str(st[3]) 
											if len(lockwaitingtid)>0:
												newstate=lockwaitingtid[1]
												if newstate=='r':
													newstate='read'
												if newstate=='w':
													newstate='write'	
												newlockholdingtransaction='-'+lockwaitingtid[2]
												newlockwaitingtransactions=lockwaitingtid[3:]
												db.locktable.update_one({"itemname": itemshold[i]},{"$set": {"lockholdingtid": newlockholdingtransaction,'lockstate': newstate,"lockwaitingtid":newlockwaitingtransactions}    })
												cur = db.transactiontable.find({"tid": lockwaitingtid[2]}, {'itemshold':1, '_id':0})
												st= str(cur[0]).split("'")
												tempo = str(st[3]) 
												db.transactiontable.update_one({"tid": lockwaitingtid[2]},{"$set": {"transstate": 'active',"itemshold":tempo+'-'+itemshold[i]}    })
											else:
												db.locktable.update_one({"itemname": itemshold[i]},{"$set": {"lockholdingtid": '','lockstate': ''}    })
									



		#correction introduced for waiting transactions
		def waitingcorrection():
			cur = db.locktable.find({}, {'lockwaitingtid':1, '_id':1})
			
			for temp1 in cur:
				st= str(temp1).split("'")
				lockwaitingtidinthis=str(st[3])
				
				if lockwaitingtidinthis!='':
					
					for d in range(0,len(lockwaitingtidinthis)):
						
						if lockwaitingtidinthis[d]!='-' and lockwaitingtidinthis[d]!='w' and lockwaitingtidinthis[d]!='r':
							
							db.transactiontable.update_one({"tid":lockwaitingtidinthis[d] },{"$set": {"transstate": 'waiting'}    })
		
		waitingcorrection()









		#printing on terminal and writing in output file,the lock and transaction tables
		#after each instruction encountered,whether it changes the ttables or not or is ignored
		
		print '********************************************************************************************'		
		o.write('********************************************************************************************'+'\n')
		print 'timestamp is '+str(timestamp) + ' and instruction executed is '+x
		o.write('timestamp is '+str(timestamp) + ' and instruction executed is '+x+'\n')
		
		print 'transaction table'
		o.write('transaction table'+'\n')
		cur=db.transactiontable.find()
		for y in cur:
			print y
			o.write(str(y))
		o.write('\n')
		print 'lock table'
		o.write('lock table'+'\n')
		cur=db.locktable.find()
		for y in cur:
			print y		
			o.write(str(y))
		
		print '********************************************************************************************'
		o.write('********************************************************************************************'+'\n')
	
	#the above loop ends,these statements execute when file is scanned one time
	#it makes aborted transactions active (and waiting trans active for code purposes)
	#and will keep on calling doit() function unless all transactions have finished and committed
	cur = db.transactiontable.find({}, {'transstate':1, '_id':0})
	for temp1 in cur:
		st= str(temp1).split("'")
		transstate=str(st[3])
		if transstate=='aborted':
			db.transactiontable.update_one({"transstate": 'aborted'},{"$set": {"transstate": 'active'}    })
			doit()
		if transstate=='waiting':
			db.transactiontable.update_one({"transstate": 'waiting'},{"$set": {"transstate": 'active'}    })
			doit()
	
doit()