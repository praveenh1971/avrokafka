package kafka.connect;

import akka.actor.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;

public class AkkaHello extends AbstractBehavior<AkkaHello.HelloCommand> {
    String message = "HELLOO";

    public AkkaHello(ActorContext<HelloCommand> context) {
        super(context);
    }

    @Override
    public Receive<HelloCommand> createReceive() {

        return newReceiveBuilder().onMessage(ChangeHello.class, this::ChangeHello )
                .onMessageEquals(SayHello.INSTANCE, this::SayHello).build();
    }

    public static  Behavior<HelloCommand> create(){

         return Behaviors.setup(c->Behaviors.receive(HelloCommand.class)
                 .onAnyMessage(mes->{

             return Behaviors.same();
         }).build());

    }
    public Behavior<HelloCommand> ChangeHello ( ChangeHello msg) {
        System.out.println(Thread.currentThread().getName() + message);
        this.message = msg.message;
        return this;
    }
    public Behavior<HelloCommand> SayHello ( ) {

        System.out.println(Thread.currentThread().getName() + message);
        return this;
    }

    interface  HelloCommand{ }

    static class ChangeHello implements  HelloCommand{
                private String message;
                public ChangeHello(String s){
                   this.message = s;
               }
        }
    static enum SayHello implements  HelloCommand{ INSTANCE  }


}
