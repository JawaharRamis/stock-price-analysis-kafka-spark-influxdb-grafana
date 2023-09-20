DROP TABLE IF EXISTS public.stock_info;


CREATE TABLE public.stock_info (
    Entry_Date DATE,
    Symbol VARCHAR(255),
    ShortName VARCHAR(255),
    LongName VARCHAR(255),
    Industry VARCHAR(255),
    Sector VARCHAR(255),
    MarketCap DECIMAL(18, 2),
    ForwardPE DECIMAL(18, 2),
    TrailingPE DECIMAL(18, 2),
    Currency VARCHAR(10),
    FiftyTwoWeekHigh DECIMAL(18, 2),
    FiftyTwoWeekLow DECIMAL(18, 2),
    FiftyDayAverage DECIMAL(18, 2),
    Exchange VARCHAR(50),
    ShortRatio DECIMAL(18, 2)
);
