# ============================================
# DATABASE MODELS (TABLE DEFINITIONS)
# ============================================
# This file defines the structure of your database tables
# Each class represents a table, and each attribute represents a column

from sqlalchemy import Boolean, Column, Integer, String, ForeignKey
from sqlalchemy.sql import func
from database import Base

# ============================================
# USER DETAILS TABLE
# ============================================
# This model represents the 'userDetail' table in your MySQL database
# It will store user registration information

class UserDetail(Base):
    # Define the table name in MySQL
    __tablename__ = 'userDetails'

    id = Column(Integer, primary_key=True, index=True, 
                autoincrement=True  # MySQL automatically generates the next ID
            )
    firstName = Column(String(50), nullable=False)
    lastName = Column(String(50), nullable=False)
    fullName = Column(String(120), nullable=False)

    # Email Address
    # - String(100): Text field with max 100 characters
    # - unique=True: No two users can have the same email
    # - nullable=False: This field is required (cannot be empty)
    # - index=True: Creates an index for faster email lookups
    email = Column(String(100), unique=True, nullable=False, index=True)
    password = Column(String(255), nullable=False)

    # ============================================
    # STRING REPRESENTATION
    # ============================================
    # This method defines how the object appears when printed
    # Useful for debugging
    def __repr__(self):
        return f'<UserDetail(id={self.id}, email={self.email}, name={self.fullName})>'

# ============================================
# USER location
# ============================================
class UserLocation(Base):
    __tablename__ = 'userLocation'
    # Primary key: Auto-incrementing ID for each news topic entry
    # This is unique for EVERY row, regardless of which user it belongs to 
    setting_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        index=True
    )

    user_id = Column(
        Integer,
        ForeignKey('userDetails.id', ondelete='CASCADE'),
        nullable=False,
        index=True  # Index for faster queries by user
    )

    ## Storing ISO code
    country_code = Column(String(2), nullable=False)  # "IN", "US", "JP"
    country_name = Column(String(100), nullable=False)  # India, Japan etc
    city = Column(String(100), nullable=False)  # "Mumbai"
    timezone_ = Column(String(50), nullable=False, default='UTC')  # "Asia/Kolkata"

    # Useful for debugging
    def __repr__(self):
        return f'<UserLocation(id={self.setting_id}, country={self.country_code}, city={self.city}, timezone={self.timezone_})>'

# ============================================
# NEWS TOPIC AND SCHEDULE TIME
# ============================================
class NewsTopicAndScheduleTime(Base):
    __tablename__ = 'topicAndSchedule'

    # Primary key: Auto-incrementing ID for each news topic entry
    # This is unique for EVERY row, regardless of which user it belongs to 
    news_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        index=True
    )

    user_id = Column(
        Integer,
        ForeignKey('userDetails.id', ondelete='CASCADE'),
        nullable=False,
        index=True  # Index for faster queries by user
    )

    newsTopic = Column(String(50), nullable=False)
    isCustomTopic = Column(Boolean, nullable=False, default=False)
    deliveryTime = Column(String(50), nullable=False)
    isImmediate = Column(Boolean, nullable=False, default=True)
    isScheduled = Column(Boolean, nullable=False, default=False)

    # Useful for debugging
    def __repr__(self):
        return f'<TopicAndSchedule(id={self.news_id}, newsTopic={self.newsTopic}, deliveryTime={self.deliveryTime})>'