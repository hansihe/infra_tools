use ractor::{Actor, ActorProcessingErr, ActorRef};

use super::kafka::{kafka_connect, list_topics};

pub struct ConnectionActor;

pub struct State {
    state: super::State,
}

#[ractor::async_trait]
impl Actor for ConnectionActor {
    type Msg = ();
    type State = State;
    type Arguments = (super::State,);

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        (state,): (super::State,),
    ) -> Result<State, ActorProcessingErr> {
        Ok(State { state })
    }

    async fn post_start(
        &self,
        myself: ActorRef<Self::Msg>,
        state: &mut State,
    ) -> Result<(), ActorProcessingErr> {
        let client = kafka_connect(&state.state).await.unwrap();
        let (partitions, topics) = list_topics(&state.state, &client).await.unwrap();
        let multi = client.multi_client().await.unwrap();

        Ok(())
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut State,
    ) -> Result<(), ActorProcessingErr> {
        Ok(())
    }
}
