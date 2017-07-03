from src.webapp.pyspyInit import *
from src.webapp.jsonifyDF import toJsonObj
import unittest
import json

def checkEqualJson(a,b):
	return sorted(a.items()) == sorted(b.items())


class TesttoJsonObj(unittest.TestCase):

    def setUp(self):
        self.season="2012"
        with open("test/unit/webapp/jsonfiles/overallStandings"+self.season+".json") as json_data:
            self.expectedJsonObj = json.load(json_data)
        dummyData = self.expectedJsonObj["Overall_Standings_"+self.season]
        dummyDF = sql.createDataFrame(dummyData)
        self.outputJsonObj = json.loads(json.dumps({"Overall_Standings_"+self.season: toJsonObj(dummyDF)}))


    def test_toJsonObj_method_returns_correct_result(self):
        self.assertEqual(True, checkEqualJson(self.expectedJsonObj, self.outputJsonObj))
        

if __name__ == '__main__':
    unittest.main()