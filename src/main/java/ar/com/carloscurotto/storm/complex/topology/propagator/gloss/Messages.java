package ar.com.carloscurotto.storm.complex.topology.propagator.gloss;

import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.TradeMessage;

/**
 * Contains the main and (if any) a trade comment message.
 * 
 * @author D540601
 */
public class Messages {
    private TradeMessage mainMessage;
    private TradeMessage commentMessage;

    public TradeMessage getMainMessage() {
        return mainMessage;
    }

    public void setMainMessage(TradeMessage mainMessage) {
        this.mainMessage = mainMessage;
    }

    public TradeMessage getCommentMessage() {
        return commentMessage;
    }

    public void setCommentMessage(TradeMessage commentMessage) {
        this.commentMessage = commentMessage;
    }
}