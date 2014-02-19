from celery import Celery
from sqlalchemy import func
from simpledoge import db
from simpledoge.models import RoundShare, Block, User

celery = Celery('tasks', broker='amqp://guest@localhost//')


@celery.task
def add_round_share(for_block, found_by, share_value):
    """
    add_round_share: Adds a round share to postgresql

    for_block should be an existing block id or a block instance

    found_by should be a username/wallet address

    share_value should be an integer representation of n1 shares

    """

    if isinstance(for_block, Block):
        for_block = for_block.id
    elif type(for_block) == int:
        pass
    else:
        raise TypeError

    RoundShare.create(for_block=for_block, found_by=found_by, share_value=share_value)


@celery.task
def new_round():
    """
    Add new block
    """

    return Block.create()


@celery.task
def complete_block(finished_block, block_found_by, blockheight, network_value, transaction_fees):
    """
    complete_block: Update postgresql w/ block & blockchain data

    finished_block should be a block instance or an existing block id

    block_found_by should be a username/wallet address

    blockheight should be the height of the given block in the blockchain

    network_value should be an integer representation of the value of the
                  newly discovered block. E.G.
                  DOGE = 2364681.04976814
                  network_value = 236468104976814

    transaction_fees should be an integer amount awarded due to transactions
                     handled by the block. E.G.
                     transaction fees on new block = 6.5
                     transaction_fees = 650000000

    """

    if isinstance(finished_block, Block):
        pass
    elif type(finished_block) == int:
        finished_block = Block.query.filter_by(id=finished_block).one()
    else:
        raise TypeError

    Block.complete(finished_block,
                   found_by=block_found_by,
                   blockheight=blockheight,
                   network_value=network_value,
                   transaction_fees=transaction_fees)

@celery.task
def compact_round_shares(block):

    if isinstance(block, Block):
        pass
    elif type(block) == int:
        block = Block.query.filter_by(id=block).one()
    else:
        raise TypeError

    users = db.session.query(func.distinct(RoundShare.found_by))
    print(users)
    # for user in users:
    #     db.session.query.filter_by(RoundShares.found_by = user).(RoundShares.found_at)








    








