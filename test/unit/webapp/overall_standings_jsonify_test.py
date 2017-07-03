from src.webapp.corefuncs import get_match_df, overall_rank_jsonify
import unittest
import json

def checkEqualJson(a,b):
	return sorted(a.items()) == sorted(b.items())


class TestOverallRankJsonify(unittest.TestCase):

    def setUp(self):
        self.season="2012"

        with open("test/unit/webapp/jsonfiles/overallStandings"+self.season+".json") as json_data:
            self.expectedJsonObj = json.load(json_data)

        self.dummyDF = get_match_df()
        self.outputJsonObj = json.loads(json.dumps\
            ({"Overall_Standings_"+self.season:\
             overall_rank_jsonify(self.dummyDF, self.season)}))


    def test_overall_rank_jsonify_method_returns_correct_result(self):
        self.assertEqual(True, checkEqualJson(self.expectedJsonObj, self.outputJsonObj))
        

if __name__ == '__main__':
    unittest.main()