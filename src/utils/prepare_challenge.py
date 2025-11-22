from pyspark.sql import SparkSession

from chall.easy.chall_one_easy import ChallengeOneEasy
from chall.hard.chall_upsert_df import ChallUpsertDf
from chall.medium.chall_one_medium import ChallengeOneMedium
from chall.hard.challenge_one_hard import ChallengeOneHard
from chall.easy.chall_withcolumn_or_not import ChallWithColumnOrNot
from chall.hard.challenge_two_hard import ChallengeTwoHard
from chall.medium.chall_window_one import ChallWindowOne
from chall.hard.chall_upsert_df_v2 import ChallUpsertDfV2

challenge = [
    ChallengeOneEasy,
    ChallengeOneMedium,
    ChallengeOneHard,
    ChallWithColumnOrNot,
    ChallengeTwoHard,
    ChallWindowOne,
    ChallUpsertDf,
    ChallUpsertDfV2,
]

def get_challenge(spark: SparkSession):
    for chall in challenge:
        yield chall(spark)



