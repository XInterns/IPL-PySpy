from src.webapp.corefuncs import getMatchDF, team_vs_team_jsonify
import unittest
import json

def checkEqualJson(a,b):
	return sorted(a.items()) == sorted(b.items())


class TestTeamVsTeamJsonify(unittest.TestCase):

    def setUp(self):
        self.team1="Rajasthan Royals"
        self.team2="Mumbai Indians"
        
        with open("test/unit/webapp/jsonfiles/teamVsTeam"+self.team1+"_"+self.team2+".json") as json_data:
            self.expectedJsonObj  = json.load(json_data)
        
        dummyDF = getMatchDF()
        self.outputJsonObj = json.loads(json.dumps\
            ({"Team_Vs_Team_Win_Percentage_"+self.team1+"_VS_"+self.team2:\
                team_vs_team_jsonify(dummyDF, self.team1, self.team2)}))


    def test_team_vs_team_jsonify_method_returns_correct_result(self):
        self.assertEqual(True, checkEqualJson(self.expectedJsonObj, self.outputJsonObj))
        

if __name__ == '__main__':
    unittest.main()