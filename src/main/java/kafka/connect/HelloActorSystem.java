package kafka.connect;

import akka.actor.typed.ActorSystem;

public class HelloActorSystem {
    public static void main(String[] args) {
        ActorSystem<AkkaHello.HelloCommand> actorSystem = ActorSystem.create(AkkaHello.create(), "MyHello");

        actorSystem.tell(AkkaHello.SayHello.INSTANCE);
        actorSystem.tell(AkkaHello.SayHello.INSTANCE);
        actorSystem.tell(new AkkaHello.ChangeHello("NEW HEKKIO"));
        actorSystem.tell(AkkaHello.SayHello.INSTANCE);
        actorSystem.tell(AkkaHello.SayHello.INSTANCE);



    }
}
