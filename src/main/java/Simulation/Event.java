package Simulation;

public class Event {
    private Operator sender;
    private Operator receiver;
    private int numberOfMessages;
    private double messagesSize;
    private double sendingTime;
    private double receivingTime;


    public Event(Operator sender, Operator receiver, int numberOfMessages, double messagesSize, double sendingTime, double receivingTime) {
        this.sender = sender;
        this.receiver = receiver;
        this.numberOfMessages = numberOfMessages;
        this.messagesSize = messagesSize;
        this.sendingTime = sendingTime;
        this.receivingTime = receivingTime;
    }

    public Operator getSender() {
        return sender;
    }

    public void setSender(Operator sender) {
        this.sender = sender;
    }

    public Operator getReceiver() {
        return receiver;
    }

    public void setReceiver(Operator receiver) {
        this.receiver = receiver;
    }

    public int getNumberOfMessages() {
        return numberOfMessages;
    }

    public void setNumberOfMessages(int numberOfMessages) {
        this.numberOfMessages = numberOfMessages;
    }

    public double getSendingTime() {
        return sendingTime;
    }

    public void setSendingTime(double sendingTime) {
        this.sendingTime = sendingTime;
    }

    public double getReceivingTime() {
        return receivingTime;
    }

    public void setReceivingTime(double receivingTime) {
        this.receivingTime = receivingTime;
    }

}
