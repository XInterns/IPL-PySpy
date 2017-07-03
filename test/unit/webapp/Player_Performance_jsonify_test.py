from src.webapp.corefuncs import Player_Performance_jsonify
import unittest
import json

def checkEqualJson(a,b):
	return sorted(a.items()) == sorted(b.items())


class TestPlayerPerformance(unittest.TestCase):

    def setUp(self):
        self.player="MS Dhoni"
        with open("test/unit/webapp/jsonfiles/playerPerformance"+self.player+".json") as json_data:
            self.expectedJsonObj = json.load(json_data)
        
        self.outputJsonObj = json.loads(json.dumps\
            ({"Player_Performance_"+self.player: Player_Performance_jsonify(self.player)}))


    def test_Player_Performance_jsonify_method_returns_correct_result(self):
        self.assertEqual(True, checkEqualJson(self.expectedJsonObj, self.outputJsonObj))
        

if __name__ == '__main__':
    unittest.main()