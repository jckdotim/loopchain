# Copyright 2018 ICON Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""data object for peer votes to one block"""

import logging
from enum import Enum
from collections import Counter

from loopchain import configure as conf
from loopchain.baseservice import PeerManager
import collections


VoteResult = collections.namedtuple("VoteResult", 'result, '
                                                  'total_vote_count, '
                                                  'agree_vote_peer_count, '
                                                  'total_peer_count, '
                                                  'voting_ratio')


class VoteType(Enum):
    block = 1
    leader_complain = 2


class Vote:

    def __init__(self, target_hash, audience, sign=None, vote_type=VoteType.block, data=None):
        """

        :param target_hash:
        :param audience: { peer_id : peer_info(SubscribeRequest of gRPC) }
        :param sign:
        :param vote_type:
        :return
        """

        # VoteType class
        self.__type = vote_type
        self.__target_hash = target_hash
        self.__sign = sign
        self.__data = data
        self.__votes = self.__make_vote_init(audience)
        self.__last_voters = []  # [peer_id,]

    @property
    def type(self):
        return self.__type

    @property
    def votes(self):
        return self.__votes

    @property
    def target_hash(self):
        return self.__target_hash

    def __make_vote_init(self, audience):
        if not audience:
            return {}

        if isinstance(audience, PeerManager):
            audience = list(audience.peer_list[conf.ALL_GROUP_ID].keys())

        return {peer_id: None for peer_id in audience}

    def __parse_vote_sign(self, vote_sign):
        """Derive result of vote from vote_sign."""

        return vote_sign

    def add_vote(self, peer_id, vote_sign):
        if peer_id not in self.__votes.keys():
            return False

        if self.__votes[peer_id]:
            logging.debug(f"This peer already votes.\n"
                          f"old:({peer_id} to {self.__votes[peer_id][0]})\nnew:({peer_id} to {vote_sign}) ")
            return False

        result = self.__parse_vote_sign(vote_sign)
        self.__votes[peer_id] = (result, vote_sign)
        self.__last_voters.append(peer_id)
        return True

    def get_voters(self):
        return list(set(self.__last_voters))

    def get_result(self, block_hash, voting_ratio):
        return self.get_result_detail(block_hash, voting_ratio).result

    def get_result_detail(self, target_hash, voting_ratio) -> VoteResult:
        """

        :param target_hash:
        :param voting_ratio:
        :return: result(str), total_vote_count, agree_vote_peer_count, total_peer_count, voting_ratio
        """

        total_peer_count = len(self.__votes)
        if self.__target_hash != target_hash:
            return VoteResult(
                result=None,
                total_vote_count=-1,
                agree_vote_peer_count=-1,
                total_peer_count=total_peer_count,
                voting_ratio=voting_ratio
            )

        count_list = Counter([vote[0] for vote in self.__votes.values() if vote])
        most_common = count_list.most_common(1)
        result, agree_vote_peer_count = most_common[0] if most_common else (None, 0)
        total_vote_count = sum(count_list.values())

        if agree_vote_peer_count < total_peer_count * voting_ratio:
            result = None

        logging.debug(f"==result: {result}")
        logging.debug(f"=agree_vote_peer_count: {agree_vote_peer_count}")
        logging.debug(f"=total_vote_count: {total_vote_count}")
        logging.debug(f"=total_peer_count: {total_peer_count}")

        vote_result = VoteResult(
            result=result,
            total_vote_count=total_vote_count,
            agree_vote_peer_count=agree_vote_peer_count,
            total_peer_count=total_peer_count,
            voting_ratio=voting_ratio
        )

        return vote_result

    def is_failed_vote(self, target_hash, voting_ratio):
        vote_result = self.get_result_detail(target_hash, voting_ratio)

        fail_vote_count = vote_result.total_vote_count - vote_result.agree_vote_peer_count
        possible_agree_vote_count = vote_result.total_peer_count - fail_vote_count

        if possible_agree_vote_count > vote_result.total_peer_count * voting_ratio:
            # this vote still possible get consensus
            return False
        else:
            # this vote final fail
            return True

    def set_vote_with_prev_vote(self, prev_vote):
        for group_id in list(self.__votes.keys()):
            if group_id not in prev_vote.votes.keys():
                continue
            for peer_id in list(self.__votes[group_id].keys()):
                if peer_id not in prev_vote.votes[group_id].keys():
                    continue
                self.__votes[group_id][peer_id] = prev_vote.votes[group_id][peer_id]

    def check_vote_init(self, audience):
        """check leader's vote init is same on this peer

        :param audience: { peer_id : peer_info(SubscribeRequest of gRPC) } or peer_list {}
        :return:
        """

        vote_groups = list(self.__votes.keys())
        check_groups = list(self.__make_vote_init(audience).keys())
        return vote_groups.sort() == check_groups.sort()
