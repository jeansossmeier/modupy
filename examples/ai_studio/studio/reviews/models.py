from sqlalchemy import Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Review(Base):
    __tablename__ = "reviews_review"

    review_id: Mapped[str] = mapped_column(String, primary_key=True)
    product: Mapped[str] = mapped_column(String, index=True)
    rating: Mapped[int] = mapped_column(Integer)
    text: Mapped[str] = mapped_column(String)
    # pending until moderation answers, then published or hidden
    status: Mapped[str] = mapped_column(String, default="pending")
    reply: Mapped[str | None] = mapped_column(String, default=None)
