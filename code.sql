match (n) detach delete n;

LOAD CSV with headers FROM "file:///EPL_dataset_for_2018_19_assignment.csv" AS team
MERGE (t1:Team{clubName:team.Team_1})
MERGE(t2:Team{clubName:team.Team_2})
MERGE (t1)-[:PLAYED{gameWeek:toInt(team.Round), ht:team.HT, ft:team.FT, gameDate:team.Date}]->(t2)
RETURN t1, t2;

-- Determine Home win
MATCH (t:Team)-[p:PLAYED]->(t2:Team)
WITH split(p.ft, '-') AS score, p AS p, t AS t, t2 AS t2
WHERE (toInt(score[0]) - toInt(score[1])) > 0
SET p.winner = t.clubName, p.loss = t2.clubName
RETURN t.clubName, p.winner, t2.clubName, p.gameWeek, p.loss
ORDER BY p.gameWeek;

-- Determine Away win
MATCH (t:Team)-[p:PLAYED]->(t2:Team)
WITH split(p.ft, '-') AS score, p AS p, t AS t, t2 AS t2
WHERE (toInt(score[0]) - toInt(score[1])) < 0
SET p.winner = t2.clubName, p.loss = t.clubName
RETURN t.clubName, p.winner, t2.clubName, p.gameWeek, p.loss
ORDER BY p.gameWeek;

-- Determine Half Time Home win
MATCH (t:Team)-[p:PLAYED]->(t2:Team)
WITH split(p.ht, '-') AS score, p AS p, t AS t, t2 AS t2
WHERE (toInt(score[0]) - toInt(score[1])) > 0
SET p.halfwinner = t.clubName
RETURN t.clubName, p.halfwinner, t2.clubName, p.gameWeek, p.ht
ORDER BY p.gameWeek;

-- Determine Half Time Away win
MATCH (t:Team)-[p:PLAYED]->(t2:Team)
WITH split(p.ht, '-') AS score, p AS p, t AS t, t2 AS t2
WHERE (toInt(score[0]) - toInt(score[1])) < 0
SET p.halfwinner = t2.clubName
RETURN t.clubName, p.halfwinner, t2.clubName, p.gameWeek, p.ht
ORDER BY p.gameWeek;

-- SET the win records
MATCH (t:Team)-[r:PLAYED]-()
WHERE r.winner = t.clubName
WITH count(r.winner) AS win, t AS t
SET t.win = win
RETURN win, t.clubName
ORDER BY win;

-- SET the lost records
MATCH (t:Team)-[r:PLAYED]-()
WHERE r.loss = t.clubName
WITH count(r.loss) AS loss, t AS t
SET t.loss = loss
RETURN loss, t.clubName
ORDER BY loss;

-- Qs no. 1: Total number of matches played. 
MATCH ()-[p:PLAYED]->()
RETURN COUNT(p);

-- Qs no. 2: Detailes of all the matches involving "Manchester United FC"
MATCH (t:Team{clubName:'Manchester United FC'})-[r:PLAYED]-(t2:Team)
RETURN t, r, t2
ORDER BY r.gameWeek;

-- Qs no 3: Display all the teams playing EPL season 17/18
MATCH (t:Team)
RETURN t;

-- Qs no 4: Display the team with the most “win” in January.
MATCH (t:Team)-[p:PLAYED]->()
WITH split(p.gameDate, ' ') AS gameDate, p AS p
WHERE 'Jan' IN gameDate
RETURN count(p.winner) as win, p.winner
ORDER BY win DESC;

-- Qs no 5: Display top 5 teams with best scoring records
MATCH (t:Team)-[r:PLAYED]-()
WITH split(r.ft, '-') AS score, r, t
WITH CASE
WHEN (t)-[r]->()
THEN score[0]
WHEN (t)<-[r]-()
THEN score[1]
END AS scores, r, t
RETURN sum(toInt(scores)) AS scores, t.clubName
ORDER BY scores DESC LIMIT 5;


-- Qs no 6: Display top 5 teams with worst defending records
MATCH (t:Team)-[r:PLAYED]-()
WITH split(r.ft, '-') AS score, r, t
WITH CASE
WHEN (t)-[r]->()
THEN score[1]
WHEN (t)<-[r]-()
THEN score[0]
END AS scores, r, t
RETURN sum(toInt(scores)) AS goalConceded, t.clubName
ORDER BY goalConceded DESC LIMIT 5;

-- Qs no 7: Display 5 teams with most winning records
MATCH ()-[p:PLAYED]->()
RETURN count(p.winner) AS win, p.winner
ORDER BY win DESC LIMIT 5;

-- Qs no 8: Display 5 teams with best half time records
MATCH (t:Team)-[r:PLAYED]-()
WHERE r.halfwinner = t.clubName
RETURN count(r.halfwinner) AS win, t.clubName
ORDER BY win DESC LIMIT 5;


-- Qs no 9: Display team with most loss
MATCH ()-[p:PLAYED]->()
RETURN count(p.loss) AS loss, p.loss
ORDER BY loss DESC;

-- Qs no 10: Display team with most consecutive win
/*
	REFERENCES= https://stackoverflow.com/questions/48340317/list-the-most-consecutive-wins-in-cypher
	Date: 25-Dec
*/

MATCH (t:Team)-[r:PLAYED]-()
WITH r.winner AS win, t
ORDER BY r.gameWeek
WITH collect([win]) AS collection, t
WITH reduce(winConsecutive = [], i IN range(0, size(collection)-1) | 
    CASE collection[i] = collection[i-1]
      WHEN true THEN [j IN range(0, size(winConsecutive) - 1) |
          CASE j = size(winConsecutive) - 1
            WHEN true THEN winConsecutive[j] + [collection[i]]
            ELSE winConsecutive[j]
          END
        ]
      ELSE winConsecutive + [[collection[i]]]
    END
  ) AS winningConsecutive, t
UNWIND winningConsecutive AS consecutive
WITH consecutive, t
WHERE consecutive[0] <> 0
RETURN t.clubName, max(size(consecutive)) AS consecutiveWin
ORDER BY consecutiveWin DESC LIMIT 1;



MATCH (t:Team)-[r:PLAYED]->()
WITH r.winner AS win, t
WITH collect([win]) AS collection, t

RETURN r.winner
ORDER BY r.gameWeek;

-- <-------------------------------------->

-- Query the round and the game winners of the game week
MATCH (t:Team)-[p:PLAYED]->(t2:Team)
RETURN t.clubName, t2.clubName, p.ft, p.winner
ORDER BY t.clubName;

MATCH (t:Team)<-[p:PLAYED]-(t2:Team)
RETURN t.clubName, t2.clubName, p.ft, p.winner
ORDER BY t.clubName;


MATCH (t:Team)-[r:PLAYED]->(t2:Team)
WITH split(r.ft, '-') AS score, r as r, t as t
WHERE '1' = score[1]
SET r.ft = (score[0] + '-' + 1)
RETURN r.ft AS Score, t.clubName, r.gameWeek
ORDER BY r.gameWeek;

-- {Jan:1, Feb:2, Mar:3, Apr:4, May:5, Jun:6, Jul:7} AS mapToScore
-- ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul'] AS monthToScore
/*
SET r.ft = replace(r.ft, monthToScore[i], toString(i)))
RETURN r.ft
ORDER BY r.gameWeek;
*/

RETURN [i IN RANGE(1, SIZE(monthToScore)) SET r.ft = replace(r.ft, monthToScore[i], i)][0];


-- <---------------------------------------------->


MATCH (t:Team)-[r:PLAYED]-()
WITH split(r.ft, '-') AS score, r, t
WITH CASE
WHEN (t)-[r]->()
THEN score[0]
WHEN (t)<-[r]-()
THEN score[1]
END AS scores, r, t
RETURN sum(toInt(scores)) AS scores, t.clubName
ORDER BY scores DESC;



MATCH (t:Team)-[r:PLAYED]->()
WITH split(r.ft, '-') AS score, r, t
SET r.GF = toInt(score[0])
RETURN r.GF, t.clubName, r.winner, r.ft
ORDER BY t.clubName;


MATCH (t:Team)<-[r:PLAYED]-()
WITH split(r.ft, '-') AS score, r, t
SET r.AGF = toInt(score[1])
RETURN r.AGF, t.clubName, r.winner, r.ft
ORDER BY t.clubName;


MATCH (t:Team)-[r:PLAYED]->(t2:Team)
WITH sum(r.GF) AS GF, sum(r.AGF) AS AGF, t
RETURN GF + AGF, t.clubName
ORDER BY t.clubName;


MATCH (t:Team)-[p:PLAYED]-(t2:Team)
SET p.GF = null



MATCH (t:Team)<-[r:PLAYED]-(t2:Team)
WITH split(r.ft, '-') AS score, r as r, t as t
WHERE '1' = score[1]
SET r.ft = (score[0] + '-' + 1)
RETURN r.ft AS Score, t.clubName, r.gameWeek
ORDER BY r.gameWeek;



MATCH (t:Team{clubName:'Arsenal FC'})-[r:PLAYED]-(t2:Team)
WITH [r.ft] as ft, t2 as t2, r as r
RETURN ft, t2.clubName
ORDER BY r.gameWeek;


CREATE CONSTRAINT ON (t:Team) ASSERT t.clubName IS UNIQUE;
DROP CONSTRAINT ON (t:Team) ASSERT t.clubName IS UNIQUE;


MATCH (t:Team{clubName:'Manchester City FC'})-[r:PLAYED]-(t2:Team)
RETURN t.clubName, t2.clubName, r.winner, r.gameWeek
ORDER BY r.gameWeek;