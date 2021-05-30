package com.mikerusoft.playground.models.udhi;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Test;

import static com.mikerusoft.playground.models.udhi.StamTest.PaymentMethodType.BANK_ACCOUNT;
import static com.mikerusoft.playground.models.udhi.StamTest.PaymentMethodType.CREDIT_CARD;
import static org.junit.jupiter.api.Assertions.*;

class StamTest {

    @Test
    void fgdgd() {
        CreditCardPayment paymentMethod1 = new CreditCardPayment("11111", "12/2021");
        assertEquals("11111,12/2021", PrintServiceFactory.getOne(paymentMethod1.getType()).printDetails(paymentMethod1));
        BankAccountPayment paymentMethod2 = new BankAccountPayment("22222", "44444");
        assertEquals("22222,44444", PrintServiceFactory.getOne(paymentMethod2.getType()).printDetails(paymentMethod2));
    }

    enum PaymentMethodType {
        CREDIT_CARD,
        BANK_ACCOUNT
    }

    interface PaymentMethod {
        PaymentMethodType getType();
        default void setType(PaymentMethodType type) {
            throw new UnsupportedOperationException("bla-bla");
        }
        String printDetails();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CreditCardPayment implements PaymentMethod {
        private String creditCardNo;
        private String expiration;

        @Override
        public PaymentMethodType getType() {
            return CREDIT_CARD;
        }

        @Override
        public String printDetails() {
            return creditCardNo + "," + expiration;
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BankAccountPayment implements PaymentMethod {

        private String accountNo;
        private String swiftCode;

        @Override
        public PaymentMethodType getType() {
            return BANK_ACCOUNT;
        }

        @Override
        public String printDetails() {
            return accountNo + "," + swiftCode;
        }
    }

    interface PrintService {
        default String printDetails(PaymentMethod paymentMethod) {
            return paymentMethod.printDetails();
        }
    }

    // option 2 for more complicated use case...
    public static class BankAccountPrintService implements PrintService {

    }

    public static class CreditCardPrintService implements PrintService {

    }

    public static class PrintServiceFactory {
        public static PrintService getOne(PaymentMethodType type) {
            switch (type) {
                case CREDIT_CARD:
                    return new CreditCardPrintService();
                case BANK_ACCOUNT:
                    return new BankAccountPrintService();
            }
            throw new IllegalArgumentException("Not supported");
        }
    }

}