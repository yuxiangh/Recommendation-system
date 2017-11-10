from pyspark import SparkContext
import math
import pyspark as ps
import sys

ratingsFile = sys.argv[1]
testFile=sys.argv[2]

sc = SparkContext("local[1]", "Simple App")
ratingsData = sc.textFile(ratingsFile)
testingData= sc.textFile(testFile)

rate=ratingsData.map(lambda x:x.split(",")).map(lambda x:(x[0],x[1],x[2]))
clean_set=rate.filter(lambda x:x[0]!="userId")
cleanset=clean_set.map(lambda x:((int(x[0]),int(x[1])),float(x[2])))
##print cleanset.take(5)

test=testingData.map(lambda x:x.split(",")).map(lambda x:(x[0],x[1], "filtered"))
clean_test=test.filter(lambda x:x[0]!="userId")
cleantest=clean_test.map(lambda x:((int(x[0]),int(x[1])),str(x[2])))

merged_table=cleanset.leftOuterJoin(cleantest).sortByKey()

part_train_data=merged_table.filter(lambda x:x[1][1]!="filtered")
part_test_data=merged_table.filter(lambda x:x[1][1]=="filtered")

user_train_data=part_train_data.map(lambda x:((x[0][0]),(x[0][1],x[1][0])))
User_Train_Data=user_train_data.groupByKey().mapValues(list).sortByKey().collect()


User_Train_Data_NoRate=part_train_data.map(lambda x:(x[0][0],x[0][1])).groupByKey().mapValues(list).sortByKey().collect()
#print User_Train_Data_NoRate[0]
#print User_Train_Data_NoRate[1]


part_avg_sum=part_train_data.map(lambda x:(x[0][0],x[1][0])).reduceByKey(lambda v1,v2:v1+v2).sortByKey()
part_avg_number=part_train_data.map(lambda x:(x[0][0],1)).reduceByKey(lambda v1,v2:v1+v2).sortByKey()
part_avg=part_avg_sum.join(part_avg_number).sortByKey()

avg=part_avg.map(lambda x:(x[0],(x[1][0]/x[1][1]))).sortByKey().collect()


avglist=[]
for item in avg:
    number=item[1]
    avglist.append(number)


train_data=part_train_data.map(lambda x:((x[0][0],x[0][1]),x[1][0])).collect()
train_data_cloud=part_train_data.map(lambda x:((x[0][0],x[0][1]),x[1][0]))

train_dict={i[0]:i[1] for i in train_data_cloud.collect()}


test_data=part_test_data.map(lambda x:((x[0][0],x[0][1]),x[1][0])).collect()
test_data_withoutvalue=part_test_data.map(lambda x:(x[0][0],x[0][1])).collect()
movieId_allUser=train_data_cloud.map(lambda x:(x[0][1],x[0][0])).sortByKey().groupByKey().mapValues(list).collect()


def constructUserMovieRateDict(traindata):
    userdict={}
    for userMovieRate in traindata:
        userId=userMovieRate[0]
        movierate=dict(userMovieRate[1])
        userdict.update({userId:movierate})
    return userdict
UserMovieRate=constructUserMovieRateDict(User_Train_Data)



def pearson_correlation(user1,user2):
    user1avg=0
    user2avg=0
    cocount=0

    for movie,rate in user1.items():
        if movie in user2:
            cocount=cocount+1
            user1avg=user1avg+rate
            user2avg=user2avg+user2[movie]

    if cocount==0:
        return 0

    user1avg=user1avg/cocount
    user2avg=user2avg/cocount

    numerator=0
    user1denominator=0
    user2denominator=0

    for movie,rate in user1.items():
        if movie in user2:
            numerator=numerator+(rate-user1avg)*(user2[movie]-user2avg)
            user1denominator=user1denominator+(rate-user1avg)**2
            user2denominator=user2denominator+(user2[movie]-user2avg)**2
    if user1denominator==0 or user2denominator==0:
        return 0

    return numerator/(math.sqrt(user1denominator)*math.sqrt(user2denominator))

def getPearsonMatrix(usermovierate):
    i=1
    alldict={}
    while i<len(usermovierate)+1:
        firstuser=usermovierate[i]
        j=1
        while j<len(usermovierate)+1:
            seconduser=usermovierate[j]
            pearson=pearson_correlation(firstuser,seconduser)
            alldict.update({(i,j):pearson})
            j=j+1
        i=i+1
    return alldict
alluserPearson=getPearsonMatrix(UserMovieRate)

def getMatchList(moviedata,testdata):
    i=0
    wholelist=[]
    while i<len(test_data):
        userid=testdata[i][0]
        movieid=testdata[i][1]
        matchlist=[]
        matchlist.append(userid)
        matchlist.append(movieid)
        j=0
        while j<len(moviedata):
            movieIndex=moviedata[j][0]
            if movieid==movieIndex:
                matchlist.append(moviedata[j][1])
            j=j+1
        wholelist.append(matchlist)
        i=i+1
    return wholelist
MatchList=getMatchList(movieId_allUser,test_data_withoutvalue)

def getFiveMaxValue(list1,list2):
    maxnumberlist=[]
    maxnumberindexlist=[]
    candidatelist=[]
    origniallist = list1
    if len(list2)>=10:
        for i in range(0, 10):
            maxnumber = max(list1)
            maxnumberindex = origniallist.index(maxnumber)
            maxnumberlist.append(maxnumber)
            maxnumberindexlist.append(maxnumberindex)
            list1.remove(maxnumber)
        for item in maxnumberindexlist:
            candidatelist.append(list2[item])
    else:
        for i in range(0,len(list2)):
            maxnumber = max(list1)
            maxnumberindex = origniallist.index(maxnumber)
            maxnumberlist.append(maxnumber)
            maxnumberindexlist.append(maxnumberindex)
            list1.remove(maxnumber)
        for item in maxnumberindexlist:
            candidatelist.append(list2[item])
    return maxnumberlist,candidatelist

def getMaxPeason(matchlist):
    i=0
    whole_pearson_list=[]
    whole_matchid_list=[]
    whole_realvalue_list=[]
    whole_candidate_avg_list=[]
    while i<len(matchlist):
        if len(matchlist[i])==3:
            userid=matchlist[i][0]
            movid=matchlist[i][1]
            matchpersonlist=matchlist[i][2]
            j=0
            pearsonlist=[]
            while j<len(matchpersonlist):
                matchpersonid=matchpersonlist[j]
                pearson=alluserPearson[(userid,matchpersonid)]
                pearsonlist.append(pearson)
                j=j+1
            topFivePearsonAndCandidate=getFiveMaxValue(pearsonlist,matchpersonlist)
            topFivePearson=topFivePearsonAndCandidate[0]
            topFiveCandidateId=topFivePearsonAndCandidate[1]

            realValueList=[]
            candidateAvgList=[]
            for candidateid in topFiveCandidateId:
                realvalue=train_dict[(candidateid,movid)]
                realValueList.append(realvalue)
                candidateAvg=avglist[candidateid-1]
                candidateAvgList.append(candidateAvg)

            whole_pearson_list.append(topFivePearson)
            whole_matchid_list.append(topFiveCandidateId)
            whole_realvalue_list.append(realValueList)
            whole_candidate_avg_list.append(candidateAvgList)
        else:
            whole_pearson_list.append("nomatch")
            whole_matchid_list.append("nomatch")
            whole_realvalue_list.append("nomatch")
            whole_candidate_avg_list.append("nomatch")
        i=i+1
    return whole_matchid_list,whole_pearson_list,whole_realvalue_list,whole_candidate_avg_list
CandidateResult=getMaxPeason(MatchList)

Matchid_List=CandidateResult[0]
Pearson_List=CandidateResult[1]
Realvalue_List=CandidateResult[2]
Candidate_Avg_list=CandidateResult[3]


def predictList(matchlist,realvaluelist,candidateavglist,peasonlist):
    i=0
    predictlist=[]
    while i<len(matchlist):
        userid=matchlist[i][0]
        userScore=avglist[userid-1]

        real_list=realvaluelist[i]
        candidate_avg_list=candidateavglist[i]
        peason_list=peasonlist[i]
        if real_list!="nomatch":
            j=0
            numerator=0
            denominator=0
            while j<len(real_list):
                numerator=numerator+(real_list[j]-candidate_avg_list[j])*peason_list[j]
                denominator=denominator+abs(peason_list[j])
                j=j+1
            if denominator==0:
                predict=userScore
            else:
                predict = userScore + numerator / denominator
        else:
            predict=userScore
        predictlist.append(predict)
        i=i+1
    return predictlist
PredictList=predictList(MatchList,Realvalue_List,Candidate_Avg_list,Pearson_List)


def improvePredictList(predictlist):
   i=0
   goodlist=[]
   while i<len(predictlist):
       item=predictlist[i]
       if item>5:
           item=4
       if item<0:
           item=1
       goodlist.append(item)
       i=i+1
   return goodlist
Improved_List=improvePredictList(PredictList)

def realValueList(testdata):
    i=0
    testdatalist=[]
    while i<len(testdata):
        realvalue=testdata[i][1]
        testdatalist.append(realvalue)
        i=i+1
    return testdatalist
RealValueList=realValueList(test_data)
print len(RealValueList)

def CalculateMSE(lista,listb):
    j=0
    sum=0
    number_1=0
    number_2=0
    number_3=0
    number_4=0
    number_5=0
    while j<len(lista):
        square=(lista[j]-listb[j])**2
        sum=sum+square
        if abs(lista[j]-listb[j])>=0 and  abs(lista[j]-listb[j])<=1:
            number_1=number_1+1
        elif  abs(lista[j]-listb[j])>1 and  abs(lista[j]-listb[j])<=2:
            number_2=number_2+1
        elif  abs(lista[j]-listb[j])>=2 and  abs(lista[j]-listb[j])<=3:
            number_3=number_3+1
        elif  abs(lista[j]-listb[j])>=3 and  abs(lista[j]-listb[j])<=4:
            number_4=number_4+1
        else:
            number_5=number_5+1
        j=j+1
    MSE=sum/len(lista)
    RMSE=math.sqrt(MSE)
    return RMSE,number_1,number_2,number_3,number_4,number_5
mse=CalculateMSE(RealValueList,Improved_List)

print ("RMSE="+str(mse[0]))
print (">=0 and <=1:"+str(mse[1]))
print (">=1 and <=2:"+str(mse[2]))
print (">=2 and <=3:"+str(mse[3]))
print (">=3 and <=4:"+str(mse[4]))
print (">=4:"+str(mse[5]))
































