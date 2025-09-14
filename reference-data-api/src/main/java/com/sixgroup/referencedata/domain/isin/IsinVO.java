package com.sixgroup.referencedata.domain.isin;

import java.time.LocalDate;
import java.util.Objects;

import org.apache.commons.validator.routines.ISINValidator;

public record IsinVO(String isin,
                     LocalDate maturityDate,
                     String currency,
                     String cfi) {

    public IsinVO {
        if (Objects.isNull(isin) || isin.trim().isEmpty()) {
            throw new IllegalArgumentException("ISIN cannot be null or empty");
        }
        if (!isValidIsin(isin)) {
            throw new IllegalArgumentException("ISIN is not valid");
        }
        if (currency == null || currency.length() != 3) {
            throw new IllegalArgumentException("Currency must be a 3-character ISO code");
        }
        if (cfi == null || cfi.length() != 6) {
            throw new IllegalArgumentException("CFI must be exactly 6 characters long");
        }
    }

    private boolean isValidIsin(String isin) {
        if (isin == null || isin.length() != 12) {
            return false;
        }

        ISINValidator validator = ISINValidator.getInstance(true);
        return validator.isValid(isin);
    }
}
