import pyspark
from pyspark import SparkContext 
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import pyspark.sql.functions as func
from pyspark.sql.functions import udf

import pandas as pd
import numpy as np

sc = SparkContext()        # creating sparkcontext
sql = SQLContext(sc)       # creating SQLcontext

data_opath = "../output/csv/"

seasonMin = 2008
seasonMax = 2016
joinCol = "name"
BatColList = ["run0", "run1", "run2", "run3", "run4", "run6"]
BallColList = ["run0", "run1", "run2", "run3", "run4", "run5", "run6", "wickets"]

def getColumnList(srcDF):
    return srcDF.columns


def getDF2(ratingType, filename):
    resultRDD = sc.textFile(data_opath + filename)
    resultHeader = resultRDD.filter(lambda l : ratingType in l)
    resultRDDNoHeader = resultRDD.subtract(resultHeader)
    if(ratingType == "batsman"):
        resultTempRDD = resultRDDNoHeader.map(lambda k : k.split(',')).map(lambda p: (p[0],p[1], int(p[2]), int(p[3]),int(p[4]), int(p[5]), int(p[6]),int(p[7]), int(p[8]), round(float(p[9]),2)))
        resultDF = sql.createDataFrame(resultTempRDD, resultRDD.first().split(','))
        resultDF = resultDF.drop('')
        resultDF = resultDF.select(ratingType, "run0", "run1", "run2", "run3", "run4", "run6",)
    elif(ratingType == "bowler"):
        resultTempRDD = resultRDDNoHeader.map(lambda k : k.split(',')).map(lambda p: (p[0],p[1], int(p[2]), int(p[3]),int(p[4]), int(p[5]), int(p[6]),int(p[7]), int(p[8]), int(p[9]), int(p[10]), round(float(p[11]),2)))
        resultDF = sql.createDataFrame(resultTempRDD, resultRDD.first().split(','))
        resultDF = resultDF.drop('')
        resultDF = resultDF.select(ratingType, "run0", "run1", "run2", "run3", "run4", "run5", "run6", "wickets")
   
    resultDF = resultDF.dropDuplicates([ratingType])
    return resultDF


def getPreSumDF(ratingType, filename):
    resultRDD = sc.textFile(data_opath + filename)
    resultHeader = resultRDD.filter(lambda l : ratingType in l)
    resultRDDNoHeader = resultRDD.subtract(resultHeader)
    resultTempRDD = resultRDDNoHeader.map(lambda k : k.split(',')).map(lambda p: (p[0],p[1], int(p[2]), int(p[3]),int(p[4]), int(p[5]), int(p[6]), int(p[7]), int(p[8]), int(p[9]), int(p[10])))
    resultDF = sql.createDataFrame(resultTempRDD, resultRDD.first().split(','))
    resultDF = resultDF.drop('')
    return resultDF


def calcRangeRating(season1, season2, season3, season4, season5, season6, season7, season8, season9, seasonLBound, seasonUBound):
    ratingsList = [season1, season2, season3, season4, season5, season6, season7, season8, season9]
    n = seasonUBound - seasonLBound + 1
    idx1 = seasonLBound - seasonMin - 1
    idx2 = seasonUBound - seasonMin
    if(seasonLBound == seasonMin):
        rangeRating = ratingsList[idx2]
    else:
        rangeRating = ratingsList[idx2] - ratingsList[idx1]
        
    rangeRating = rangeRating - (n-1)*50
    if(rangeRating < 0 ):
        rangeRating = 0
    return rangeRating


def getRangeRatings(srcDF, ratingType, seasonLBound, seasonUBound):
    calcRangeRatingUDF = udf(calcRangeRating, LongType())
    srcColList = getColumnList(srcDF)
    resultDF = srcDF.withColumn(ratingType+"rangerating", calcRangeRatingUDF(srcColList[1], srcColList[2], srcColList[3], srcColList[4], srcColList[5], srcColList[6], srcColList[7], srcColList[8], srcColList[9], func.lit(seasonLBound), func.lit(seasonUBound)))
    resultDF = resultDF.select(ratingType,ratingType+"rangerating").withColumnRenamed(ratingType,"name")
    return resultDF


def fixNoneValuesLong(score):
    if(score == None):
        return 0
    else:
        return score


def fixNoneValuesFloat(rateType, score):
    if(score == None):
        if(rateType == "strikerate"):
            return 0.0
        else:
            return 100.0
    else:
        return score

    
def fixJoinDF(srcDF, colName):
    fixNoneValuesUDF = udf(fixNoneValuesLong, LongType())
    return srcDF.withColumn(colName,fixNoneValuesUDF(srcDF[colName]))


def fixJoinDF2(srcDF, colName):
    fixNoneValuesUDF = udf(fixNoneValuesFloat, FloatType())
    return srcDF.withColumn(colName,fixNoneValuesUDF(func.lit(colName), srcDF[colName]))

    
def joinAllRatingsDF(aDF, bDF, cDF, joinCol, colList):
    aDF = aDF.join(bDF, joinCol, "fullouter")
    aDF = aDF.join(cDF, joinCol, "fullouter")
    for col in colList:
        aDF = fixJoinDF(aDF, col)
    return aDF


def joinStrikeEcoRatingsDF(aDF, bDF, cDF, joinCol, colList):
    aDF = aDF.join(bDF, joinCol, "fullouter")
    aDF = aDF.join(cDF, joinCol, "fullouter")
    for col in colList:
        aDF = fixJoinDF2(aDF, col)
    # aDF = aDF.na.fill({"strikerate": 0.0, "economyrate": 100.0})
    return aDF


def calcRelativeRating(score, maxScore):
    return long((score*100.0)/maxScore)


def getMaxRating(srcDF, colName):
    return srcDF.agg({colName: "max"}).collect()[0][0]


def getRelativeRatingsDF(srcDF, ratingTypes):
    calcRelativeRatingUDF = udf(calcRelativeRating, LongType())
    baseColName = "rangerating"
    selectList =["name"]
    for types in ratingTypes:
        colName = types+baseColName
        selectList.append(types+"rating")
        maxRating = getMaxRating(srcDF, colName)
        srcDF = srcDF.withColumn(types+"rating", calcRelativeRatingUDF(srcDF[colName], func.lit(maxRating)))
    return srcDF.select(selectList)


def calcAdditionalRatings(score1, score2):
    return score1 + score2


def getAdditionalRatingsDF(srcDF, ratingTypes):
    calcAdditionalRatingsUDF = udf(calcAdditionalRatings, LongType())
    doneSet = set()
    selectList = []
    baseColName = "rating"
    for type1 in ratingTypes:
        for type2 in ratingTypes:
            thisCombo = type1+type2
            thisComboRev = type2+type1
            if(type1 != type2 and thisCombo not in doneSet and thisComboRev not in doneSet):
                doneSet.add(thisCombo)
                srcDF = srcDF.withColumn(type1+type2+"rating", calcAdditionalRatingsUDF(srcDF[type1+baseColName], srcDF[type2+baseColName]))
    return srcDF


def calcStrikeRate(hit0, hit1, hit2, hit3, hit4, hit6):
    totalBalls = hit0 + hit1 + hit2 + hit3 + hit4 + hit6
    totalRuns = hit1 + 2*hit2 + 3*hit3 + 4*hit4 + 6*hit6
    SR = (totalRuns*100.0)/totalBalls
    return float(round(SR,2))


def calcEconomyRate(ball0, ball1, ball2, ball3, ball4, ball5, ball6):
    totalBalls = ball0 + ball1 + ball2 + ball3 + ball4 + ball5 + ball6
    totalOvers = totalBalls/6
    partialOver = totalBalls%6
    totalOvers = totalOvers+(partialOver/10.0)
    totalRuns = ball1 + 2*ball2 + 3*ball3 + 4*ball4 + 5*ball5 + 6*ball6
    if(totalOvers):
        ER = totalRuns/totalOvers
    else:
        ER = 0
    return float(round(ER,2))


def getStrikeRatesDF(ratingType, srcDF):
    calcStrikeRateUDF = udf(calcStrikeRate, FloatType())
    resultDF = srcDF.withColumn("strikerate",calcStrikeRateUDF(srcDF.run0, rcDF.run1, srcDF.run2, srcDF.run3, srcDF.run4, srcDF.run6))
    for col in BatColList:
        resultDF = resultDF.drop(col)
    resultDF = resultDF.withColumnRenamed(ratingType, joinCol)
    return resultDF.sort("strikerate",ascending=0)


def getEconomyRatesDF(ratingType, srcDF):
    calcEcoRateUDF = udf(calcEconomyRate, FloatType())
    resultDF = srcDF.withColumn("economyrate", calcEcoRateUDF(srcDF.run0,srcDF.run1, srcDF.run2, srcDF.run3, srcDF.run4, srcDF.run5, srcDF.run6,))
    for col in BallColList:
        resultDF = resultDF.drop(col)
    resultDF = resultDF.withColumnRenamed(ratingType, joinCol)
    return resultDF.sort("economyrate")


def joinStrikeRatesDF(ratingType, aDF, bDF):
    aDF = aDF.join(bDF, ratingType, "fullouter")
    for col in aDF.columns:
        if(col!=ratingType):
            aDF = fixJoinDF(aDF, col)
    return aDF


def joinEcoRatesDF(ratingType, aDF, bDF):
    aDF = aDF.join(bDF, ratingType, "fullouter")
    for col in aDF.columns:
        if(col!=ratingType):
            aDF = fixJoinDF(aDF, col)
    return aDF


def renameDF(ratingType, srcDF):
    srcDF = srcDF.toDF(*((col+"2") if(col!=ratingType) else ratingType for col in srcDF.columns))
    return srcDF


def addBatsmanDF(srcDF):
    for col in BatColList:
        srcDF = srcDF.withColumn(col, srcDF[col] + srcDF[(col+"2")])
        srcDF = srcDF.drop(col+"2")
    return srcDF


def addBowlerDF(srcDF):
    for col in BallColList:
        srcDF = srcDF.withColumn(col, srcDF[col] + srcDF[(col+"2")])
        srcDF = srcDF.drop(col+"2")
    return srcDF


def getDreamTeamRatings(seasonLBound, seasonUBound):
    ratingTypes = ["batsman", "bowler", "fielder"]
    ratingDFList = []
    for types in ratingTypes:
        filename = types+"preSumOverall"+str(seasonMin)+"_"+str(seasonMax)+".csv"
        srcDF = getPreSumDF(types, filename)
        srcDF = getRangeRatings(srcDF, types, seasonLBound, seasonUBound)
        ratingDFList.append(srcDF)

        if(types == "batsman"):
            for i in range(seasonLBound, seasonUBound+1):
                filename = types+str(i)+".csv"
                if(i==seasonLBound):
                    resDF1 = getDF2(types, filename)
                else:
                    subResDF = getDF2(types, filename)
                    subResDF = renameDF(types, subResDF)
                    resDF1 = joinStrikeRatesDF(types, resDF1, subResDF)
                    resDF1 = addBatsmanDF(resDF1)
            strikeRateDF = getStrikeRatesDF(types, resDF1)
        elif(types == "bowler"):
            for i in range(seasonLBound, seasonUBound+1):
                filename = types+str(i)+".csv"
                if(i==seasonLBound):
                    resDF1 = getDF2(types, filename)
                else:
                    subResDF = getDF2(types, filename)
                    subResDF = renameDF(types, subResDF)
                    resDF1 = joinEcoRatesDF(types, resDF1, subResDF)
                    resDF1 = addBowlerDF(resDF1)
            ecoRateDF = getEconomyRatesDF(types, resDF1)

    colList = ["batsmanrangerating", "bowlerrangerating", "fielderrangerating"]
    srcDF = joinAllRatingsDF(ratingDFList[0], ratingDFList[1], ratingDFList[2], joinCol, colList)
    srcDF = getRelativeRatingsDF(srcDF, ratingTypes)
    srcDF = getAdditionalRatingsDF(srcDF, ratingTypes)
    colList = ["strikerate", "economyrate"]
    srcDF = joinStrikeEcoRatingsDF(srcDF, strikeRateDF, ecoRateDF, joinCol, colList)   
    return srcDF


def choosePlayers(team, srcDF, reqCount):
    rowCtr = 0
    currCount = 0
    currWindow = srcDF.collect()
    currWindowSize = srcDF.count()
    currWindowTeam = []
    for i in range(len(team)):
        currWindowTeam.append(team[i][0])
        
    while( (currCount < reqCount) & (rowCtr < currWindowSize) ):
        playerName = currWindow[rowCtr][0]
        if playerName not in currWindowTeam:
            team.append(currWindow[rowCtr])
            currCount+=1
        rowCtr+=1


def pickTopBatsman(team, srcDF):
    batsmanCount = 4
    openerCount = 1
    srcDF = srcDF.sort("batsmanrating", ascending=0)
    srcDF = srcDF.limit(15)
    choosePlayers(team, srcDF, batsmanCount)
    srcDF = srcDF.sort("strikerate", ascending=0)
    # srcDF.show()
    choosePlayers(team, srcDF, openerCount)


def pickAllRounder1(team, srcDF):
    allRounder1Count = 1 
    srcDF = srcDF.sort("batsmanfielderrating", ascending=0)
    srcDF.limit(15)
    choosePlayers(team, srcDF, allRounder1Count)


def pickAllRounder2(team, srcDF):
    allRounder2Count = 1 
    srcDF = srcDF.sort("batsmanbowlerrating", ascending=0)
    srcDF.limit(15)
    choosePlayers(team, srcDF, allRounder2Count)


def pickTopBowler(team, srcDF):
    bowlerCount = 2
    economicBowlerCount = 2
    srcDF = srcDF.sort("bowlerrating", ascending=0)
    srcDF = srcDF.limit(15)
    choosePlayers(team, srcDF, bowlerCount)
    srcDF = srcDF.sort("economyrate")
    choosePlayers(team, srcDF, economicBowlerCount)


def getDreamTeam(ratingDF, seasonLBound, seasonUBound):
    team = []
    pickTopBatsman(team, ratingDF)
    print "pass1"
    pickAllRounder1(team, ratingDF)
    print "pass2"
    pickAllRounder2(team, ratingDF)
    print "pass3"
    pickTopBowler(team, ratingDF)
    return team


for i in range(seasonMin, seasonMax):
    for j in range(i, seasonMax+1):
        print i,j
        ratingDF = getDreamTeamRatings(i, j)
        dreamTeam = getDreamTeam(ratingDF, i, j)
        newDF = sql.createDataFrame(dreamTeam, ratingDF.columns)
        newpDF = newDF.toPandas()
        for z in range(newpDF.count()[0]):
            newpDF['strikerate'][z] = round(newpDF['strikerate'][z],2)
            newpDF['economyrate'][z] = round(newpDF['economyrate'][z],2)
        filename = "dreamTeam"+str(i)+"_"+str(j)+".csv"
        newpDF.to_csv(data_opath + filename)
