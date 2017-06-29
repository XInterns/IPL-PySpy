from src.webapp.jsonifyDF import sc, sql, getMatchDF, consistency_jsonify
import unittest
import json

def checkEqualJson(a,b):
	return sorted(a.items()) == sorted(b.items())


class TestConsistencyJsonify(unittest.TestCase):

    def setUp(self):
        self.seasonStart="2009"
        self.seasonEnd="2012"
        with open("test/unit/webapp/jsonfiles/performanceConsistency"+self.seasonStart+"_"+self.seasonEnd+".json") as json_data:
            self.expectedJsonObj = json.load(json_data)
        
        self.dummyDF = getMatchDF()
        self.outputJsonObj = json.loads(json.dumps\
            ({"Performance_Consistency_"+self.seasonStart+"_to_"+self.seasonEnd:\
            consistency_jsonify(self.dummyDF, self.seasonStart, self.seasonEnd)}))


    def test_consistency_jsonify_method_returns_correct_result(self):
        self.assertEqual(True, checkEqualJson(self.expectedJsonObj, self.outputJsonObj))
        

if __name__ == '__main__':
    unittest.main()