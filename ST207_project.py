# ST207 DATABASES PROJECT



##########################
# IMPORTING SQLITE3 LIBRARY AND ESTABLISHING CONNECTION
##########################

# importing sqlite3 library
import sqlite3

# importing pandas library for better viewing of SQL queries and handling of imported data
import pandas

# creating a connection to the database
conn = sqlite3.connect(':memory:')

# opening a cursor to the database
cursor = conn.cursor()


##########################
# DATABASE CREATION
##########################

# This enables the foreign key constraint to actually work in practice
cursor.execute('pragma foreign_keys=ON;')

### DDL commands creating tables and the relationships between them:
cursor.executescript(
    """
    CREATE TABLE User_Profiles (
        id INTEGER NOT NULL, 
        username VARCHAR(50) NOT NULL UNIQUE, 
        email VARCHAR(50) NOT NULL UNIQUE, 
        first_name VARCHAR(30) NOT NULL, 
        last_name VARCHAR(30) NOT NULL, 
        gender VARCHAR(30) NOT NULL, 
        privacy BOOLEAN NOT NULL, 
        password VARCHAR(30) NOT NULL,
        date_of_birth DATE NOT NULL,
        phone CHAR(10), 
        country VARCHAR(60),
        biography TEXT(300),

        PRIMARY KEY (id)
    );
    """
)

cursor.executescript(
    """
    CREATE TABLE Groups (
        id INTEGER NOT NULL, 
        creator_id INTEGER,
        groupname VARCHAR(50) NOT NULL UNIQUE, 
        title VARCHAR(50) NOT NULL,
        description TEXT(300) NOT NULL,

        PRIMARY KEY (id)
        FOREIGN KEY(creator_id) REFERENCES User_Profiles (id)
    );
    """
)

cursor.executescript(
    """
    CREATE TABLE Posts (
        id INTEGER NOT NULL, 
        user_profile_id INTEGER NOT NULL,
        content VARCHAR(255) NOT NULL, 
        created_on DATETIME,

        PRIMARY KEY (id),
        FOREIGN KEY(user_profile_id) REFERENCES User_Profiles (id)
    );
    """
)

cursor.executescript(
    """
    CREATE TABLE Comments (
        id INTEGER NOT NULL, 
        user_profile_id INTEGER NOT NULL,
        post_id INTEGER NOT NULL,
        content VARCHAR(255) NOT NULL, 
        created_on DATETIME,

        PRIMARY KEY (id),
        FOREIGN KEY(user_profile_id) REFERENCES User_Profiles (id), 
        FOREIGN KEY(post_id) REFERENCES Posts (id)
    );
    """
)

cursor.executescript(
    """
    CREATE TABLE Post_Reactions (
        post_id INTEGER NOT NULL, 
        user_profile_id INTEGER NOT NULL,
        reaction_type VARCHAR(30) NOT NULL,

        CHECK(reaction_type IN ('like', 'love', 'dislike')),

        CONSTRAINT pk_post_reaction PRIMARY KEY (post_id, user_profile_id),
        FOREIGN KEY(user_profile_id) REFERENCES User_Profiles (id), 
        FOREIGN KEY(post_id) REFERENCES Posts (id)
    );
    """
)

cursor.executescript(
    """
    CREATE TABLE Comment_Reactions (
        comment_id INTEGER NOT NULL, 
        user_profile_id INTEGER NOT NULL,
        reaction_type VARCHAR(30) NOT NULL,

        CHECK(reaction_type IN ('like', 'love', 'dislike')),

        CONSTRAINT pk_comment_reaction PRIMARY KEY (comment_id, user_profile_id),
        FOREIGN KEY(user_profile_id) REFERENCES User_Profiles (id), 
        FOREIGN KEY(comment_id) REFERENCES Comments (id)
    );
    """
)

cursor.executescript(
    """
    CREATE TABLE Group_Posts (
        post_id INTEGER NOT NULL UNIQUE, 
        group_id INTEGER NOT NULL,

        CONSTRAINT pk_group_post PRIMARY KEY (post_id, group_id),
        FOREIGN KEY(post_id) REFERENCES Posts (id), 
        FOREIGN KEY(group_id) REFERENCES Groups (id)
    );
    """
)

cursor.executescript(
    """
    CREATE TABLE Group_Members (
        user_profile_id INTEGER NOT NULL, 
        group_id INTEGER NOT NULL,
        admin BOOLEAN NOT NULL,

        CONSTRAINT pk_group_member PRIMARY KEY (user_profile_id, group_id),
        FOREIGN KEY(user_profile_id) REFERENCES User_Profiles (id), 
        FOREIGN KEY(group_id) REFERENCES Groups (id)
    );
    """
)

cursor.executescript(
    """
    CREATE TABLE Banned_Profiles (
        user_profile_id INTEGER NOT NULL, 
        group_id INTEGER NOT NULL,

        CONSTRAINT pk_banned_member PRIMARY KEY (user_profile_id, group_id),
        FOREIGN KEY(user_profile_id) REFERENCES User_Profiles (id), 
        FOREIGN KEY(group_id) REFERENCES Groups (id)
    );
    """
)

cursor.executescript(
    """
    CREATE TABLE Blocked (
        user_profile_id INTEGER NOT NULL, 
        blocked_id INTEGER NOT NULL,

        CHECK (user_profile_id != blocked_id),

        CONSTRAINT pk_blocked_person PRIMARY KEY (user_profile_id, blocked_id),
        FOREIGN KEY(user_profile_id) REFERENCES User_Profiles (id), 
        FOREIGN KEY(blocked_id) REFERENCES User_Profiles (id)
    );
    """
)

cursor.executescript(
    """
    CREATE TABLE Followers (
        user_profile_id INTEGER NOT NULL, 
        follower_id INTEGER NOT NULL,
        accepted BOOLEAN NOT NULL,

        CHECK (user_profile_id != follower_id),

        CONSTRAINT pk_follower PRIMARY KEY (user_profile_id, follower_id),
        FOREIGN KEY(user_profile_id) REFERENCES User_Profiles (id), 
        FOREIGN KEY(follower_id) REFERENCES User_Profiles (id)
    );
    """
)

cursor.executescript(
    """
    CREATE TABLE Following (
        user_profile_id INTEGER NOT NULL, 
        following_id INTEGER NOT NULL,
        accepted BOOLEAN NOT NULL,

        CHECK (user_profile_id != following_id),

        CONSTRAINT pk_following PRIMARY KEY (user_profile_id, following_id),
        FOREIGN KEY(user_profile_id) REFERENCES User_Profiles (id), 
        FOREIGN KEY(following_id) REFERENCES User_Profiles (id)
    );
    """
)

#############
#VIEWS
#############

#VIEW 1: Shows posts on groups so that it can be filtered by group_id and shown on group page
cursor.executescript(
    """
    CREATE VIEW posts_in_group AS
    SELECT post_id,group_id,content,created_on
    FROM Posts
    INNER JOIN Group_Posts
    ON Posts.id = Group_Posts.post_id;
    """
)

#VIEW 2: Shows posts not linked to groups so that it could be shown on the user's timeline filtered by used_id
cursor.executescript(
    """
    CREATE VIEW Timeline_posts AS
    SELECT id,content,created_on
    FROM Posts
    Left JOIN Group_Posts
    ON Posts.id = Group_Posts.post_id
    WHERE group_id is NULL;
    """
)

#VIEW 3: Shows reaction counts for posts to be shown the performance of the post
cursor.executescript(
    """
    CREATE VIEW Post_Reactions_Count AS
    SELECT post_id,reaction_type,COUNT(Post_reactions.user_profile_id) as number
    FROM Post_Reactions
    GROUP BY post_id,reaction_type;
    """
)

#VIEW 4: Shows reaction counts for comments
cursor.executescript(
    '''
    CREATE VIEW Comment_Reactions_count AS
    SELECT comment_id,reaction_type,COUNT(Comment_Reactions.user_profile_id) as number
    FROM Comment_Reactions
    GROUP BY comment_id,reaction_type;
    '''
)

#VIEW 5: Shows number of followers for a particular user.
cursor.executescript(
    '''
    CREATE VIEW Followers_count AS
    SELECT user_profile_id,COUNT(Followers.follower_id) as number
    FROM Followers
    GROUP BY user_profile_id;

    '''
)
####################
# TRIGGERS
####################

# Creation of triggers to maintain the consistency of the database according to the rules set out in the description.

# TRIGGER 1: The creator of a group is made a group member with admin status immediatly after creating the group.
cursor.executescript(
    """
    CREATE TRIGGER make_creator_admin_member AFTER INSERT ON Groups
    BEGIN
        INSERT INTO Group_Members (user_profile_id, group_id, admin)
        VALUES (NEW.creator_id, NEW.id, TRUE);
    END;
    """
)

# TRIGGER 2: There must be at least one group admin in each group. We cannot remove the admin status of the only admin in a group.
cursor.executescript(
    """
    CREATE TRIGGER at_least_one_admin_update BEFORE UPDATE ON Group_Members
    BEGIN
        SELECT CASE
        WHEN (
            (SELECT COUNT(*) 
            FROM Group_Members 
            WHERE Group_Members.group_id = OLD.group_id AND Group_Members.admin = TRUE) - 
            (SELECT CAST(1 AS INTEGER) WHERE OLD.admin = TRUE AND NEW.admin = FALSE) < 1
            )
        THEN RAISE(FAIL, "ERROR: You cannot remove this member's admin status. A group must have at least one member who is an admin.")
    END;
    END;
    """
)


# TRIGGER 3: There must be at least one group admin in each group. If the only admin for a Group is deleted from the Group_Members table, then the corresponding Group must also be deleted.
cursor.executescript(
    """
    CREATE TRIGGER at_least_one_admin_delete BEFORE DELETE ON Group_Members
    BEGIN
        DELETE FROM Groups
        WHERE (
            (SELECT COUNT(*)
            FROM Group_Members
            WHERE Group_Members.group_id = OLD.group_id AND Group_Members.admin = TRUE) -
            (SELECT CAST(1 AS INTEGER) WHERE OLD.admin = TRUE) < 1
        ) AND
        Groups.id = OLD.group_id;
    END;
    """
)


# TRIGGER 4: This adds the created_on attribute to Posts immediately after they have been inserted into the database.
cursor.executescript(
    """
    CREATE TRIGGER post_created_on AFTER INSERT ON Posts
    BEGIN
        UPDATE Posts
        SET created_on = datetime('now')
        WHERE id = NEW.id;
    END;
    """
)


# TRIGGER 5: This adds the created_on attribute to Comments immediately after they have been inserted into the database.
cursor.executescript(
    """
    CREATE TRIGGER comment_created_on AFTER INSERT ON Comments
    BEGIN
        UPDATE Comments
        SET created_on = datetime('now')
        WHERE id = NEW.id;
    END;
    """
)


# TRIGGER 6: If a User_Profile has been blocked by another User_Profile, delete from the Following table where the ids of those two User_Profiles are the primary key.
cursor.executescript(
    """
    CREATE TRIGGER blocked_following AFTER INSERT ON Blocked
    BEGIN
        DELETE FROM Following
        WHERE (NEW.user_profile_id = Following.user_profile_id AND NEW.blocked_id = Following.following_id) 
        OR (NEW.user_profile_id = Following.following_id AND NEW.blocked_id = Following.user_profile_id);
    END;
    """
)

# TRIGGER 7: If a User_Profile has been blocked by another User_Profile, delete from the Followers table where the ids of those two User_Profiles are the primary key.
cursor.executescript(
    """
    CREATE TRIGGER blocked_followers AFTER INSERT ON Blocked
    BEGIN
        DELETE FROM Followers
        WHERE (NEW.user_profile_id = Followers.user_profile_id AND NEW.blocked_id = Followers.follower_id) 
        OR (NEW.user_profile_id = Followers.follower_id AND NEW.blocked_id = Followers.user_profile_id);
    END;
    """
)

# We also need triggers to automatically reciprocate the following / follower relationship.
# For example, if User_Profile_1 is following User_Profile_2, this is recorded in the Following table.
# However, User_Profile_1 also needs to be recorded as one of User_Profile_2's followers, so the reciprocal relationship needs to be added to the Followers table.

# TRIGGER 8: If a new tuple is inserted to the Followers table, a new tuple needs to be inserted into the Following table which represents the inverse relationship between the two User_Profiles.
# However, this should only happen if the tuple does not already exist.
cursor.executescript(
    """
    CREATE TRIGGER add_following AFTER INSERT ON Followers
    WHEN (
        SELECT COUNT(*)
        FROM Following
        WHERE Following.user_profile_id = NEW.follower_id
        AND Following.following_id = NEW.user_profile_id
    ) = 0
    BEGIN
        INSERT INTO Following (user_profile_id, following_id, accepted) 
        VALUES (NEW.follower_id, NEW.user_profile_id, NEW.accepted);
    END;
    """
)


# TRIGGER 9: If a new tuple is inserted to the Following table, a new tuple needs to be inserted into the Followers table which represents the inverse relationship between the two User_Profiles.
# However, this should only happen if the tuple does not already exist.
cursor.executescript(
    """
    CREATE TRIGGER add_follower AFTER INSERT ON Following
    WHEN (
        SELECT COUNT(*)
        FROM Followers
        WHERE Followers.user_profile_id = NEW.following_id
        AND Followers.follower_id = NEW.user_profile_id
    ) = 0
    BEGIN
        INSERT INTO Followers (user_profile_id, follower_id, accepted)
        VALUES (NEW.following_id, NEW.user_profile_id, NEW.accepted);
    END;
    """
)

# TRIGGER 10: After a new tuple is inserted to the Banned_Profiles table, the User_Profile of that tuple needs to be removed as a Group_Member.
cursor.executescript(
    """
    CREATE TRIGGER remove_member_after_banning AFTER INSERT ON Banned_Profiles
    BEGIN
        DELETE FROM Group_Members
        WHERE Group_Members.group_id = NEW.group_id 
        AND Group_Members.user_profile_id = NEW.user_profile_id;
    END;
    """
)

# TRIGGER 11: Before inserting into the Group_Members table, we need to check that the User_Profile of that entry is not banned from that group.
cursor.executescript(
    """
    CREATE TRIGGER prevent_banned_member_being_added BEFORE INSERT ON Group_Members
    BEGIN
        SELECT CASE
        WHEN ((SELECT Banned_Profiles.user_profile_id FROM Banned_Profiles WHERE Banned_Profiles.user_profile_id = NEW.user_profile_id AND Banned_Profiles.group_id = NEW.group_id) IS NOT NULL)
        THEN RAISE(FAIL, 'ERROR: This User_Profile cannot become a member of this group because they are banned.')
    END;
    END;
    """
)

# TRIGGER 12: If a tuple in the Following table has an update performed on the accepted field, the accepted field also needs to be updated for the inverse relationship which exists in the Followers table.
cursor.executescript(
    """
    CREATE TRIGGER update_accepted_followers AFTER UPDATE ON Following
    BEGIN
        UPDATE Followers
        SET accepted = NEW.accepted
        WHERE Followers.user_profile_id = NEW.following_id 
        AND Followers.follower_id = NEW.user_profile_id;
    END;
    """
)

# TRIGGER 13: If a tuple in the Followers table has an update performed on the accepted field, the accepted field also needs to be updated for the inverse relationship which exists in the Following table.
cursor.executescript(
    """
    CREATE TRIGGER update_accepted_following AFTER UPDATE ON Followers
    BEGIN
        UPDATE Following
        SET accepted = NEW.accepted
        WHERE Following.user_profile_id = NEW.follower_id 
        AND Following.following_id = NEW.user_profile_id;
    END;
    """
)


# TRIGGER 14: After inserting a tuple into the Following table, if the privacy attribute of the User_Profile which relates to the following_id of that tuple is FALSE, then automatically update the accepted field of that tuple to TRUE.
cursor.executescript(
    """
    CREATE TRIGGER accept_request_if_privacy_false_following AFTER INSERT ON Following
    BEGIN
        UPDATE Following
        SET accepted = TRUE
        WHERE Following.user_profile_id = NEW.user_profile_id 
        AND Following.following_id = NEW.following_id 
        AND (
            SELECT User_Profiles.privacy 
            FROM User_Profiles 
            WHERE User_Profiles.id = NEW.following_id
            ) = FALSE;
    END;
    """
)

# TRIGGER 15: After inserting a tuple into the Followers table, if the privacy attribute of the User_Profile which relates to the user_profile_id of that tuple is FALSE, then automatically update the accepted field of that tuple to TRUE.
cursor.executescript(
    """
    CREATE TRIGGER accept_request_if_privacy_false_followers AFTER INSERT ON Followers
    BEGIN
        UPDATE Followers
        SET accepted = TRUE
        WHERE Followers.user_profile_id = NEW.user_profile_id 
        AND Followers.follower_id = NEW.follower_id 
        AND (
            SELECT User_Profiles.privacy 
            FROM User_Profiles 
            WHERE User_Profiles.id = NEW.user_profile_id
            ) = FALSE;
    END;
    """
)

# TRIGGER 16: After deleting a tuple from the Followers table, the reciprocal relationship must also be deleted from the Following table.
cursor.executescript(
    """
    CREATE TRIGGER delete_from_following AFTER DELETE ON Followers
    BEGIN
        DELETE FROM Following
        WHERE Following.user_profile_id = OLD.follower_id 
        AND Following.following_id = OLD.user_profile_id;
    END;
    """
)

# TRIGGER 17: After deleting a tuple from the Following table, the reciprocal relationship must also be deleted from the Followers table.
cursor.executescript(
    """
    CREATE TRIGGER delete_from_followers AFTER DELETE ON Following
    BEGIN
        DELETE FROM Followers
        WHERE Followers.user_profile_id = OLD.following_id 
        AND Followers.follower_id = OLD.user_profile_id;
    END;
    """
)

# TRIGGER 18: Before deleting a User_Profile, check to see if they are the creator of a Group. If they are, set the foreign key attribute creator_id for that Group to NULL.
cursor.executescript(
    """
    CREATE TRIGGER check_group_creator_before_delete_user BEFORE DELETE ON User_Profiles
    BEGIN
        UPDATE Groups
        SET creator_id = NULL
        WHERE Groups.creator_id = OLD.id;
    END;
    """
)


# TRIGGER 19: Before deleting a User_Profile, any tuples in the Following table which contain this User_Profile's id must also be deleted.
cursor.executescript(
    """
    CREATE TRIGGER profile_delete_following BEFORE DELETE ON User_Profiles
    BEGIN
        DELETE FROM Following
        WHERE Following.user_profile_id = OLD.id 
        OR Following.following_id = OLD.id;
    END;
    """
)

# TRIGGER 20: Before deleting a User_Profile, any tuples in the Followers table which contain this User_Profile's id must also be deleted.
cursor.executescript(
    """
    CREATE TRIGGER profile_delete_followers BEFORE DELETE ON User_Profiles
    BEGIN
        DELETE FROM Followers
        WHERE Followers.user_profile_id = OLD.id 
        OR Followers.follower_id = OLD.id;
    END;
    """
)

# TRIGGER 21: Before deleting a User_Profile, any tuples in the Blocked table which contain this User_Profile's id must also be deleted.
cursor.executescript(
    """
    CREATE TRIGGER profile_delete_blocked BEFORE DELETE ON User_Profiles
    BEGIN
        DELETE FROM Blocked
        WHERE Blocked.user_profile_id = OLD.id 
        OR Blocked.blocked_id = OLD.id;
    END;
    """
)

# TRIGGER 22: Before deleting a User_Profile, any tuples in the Banned_Profiles table which contain this User_Profile's id must also be deleted.
cursor.executescript(
    """
    CREATE TRIGGER profile_delete_banned_profiles BEFORE DELETE ON User_Profiles
    BEGIN
        DELETE FROM Banned_Profiles
        WHERE Banned_Profiles.user_profile_id = OLD.id;
    END;
    """
)

# TRIGGER 23: Before deleting a User_Profile, any tuples in the Post_Reactions table which contain this User_Profile's id must also be deleted.
cursor.executescript(
    """
    CREATE TRIGGER profile_delete_post_reactions BEFORE DELETE ON User_Profiles
    BEGIN
        DELETE FROM Post_Reactions
        WHERE Post_Reactions.user_profile_id = OLD.id;
    END;
    """
)

# TRIGGER 24: Before deleting a User_Profile, any tuples in the Comment_Reactions table which contain this User_Profile's id must also be deleted.
cursor.executescript(
    """
    CREATE TRIGGER profile_delete_comment_reactions BEFORE DELETE ON User_Profiles
    BEGIN
        DELETE FROM Comment_Reactions
        WHERE Comment_Reactions.user_profile_id = OLD.id;
    END;
    """
)

# TRIGGER 25: Before deleting a Post, any tuples in the Group_Posts table which contain this Post's id must also be deleted.
cursor.executescript(
    """
    CREATE TRIGGER group_post_delete BEFORE DELETE ON Posts
    BEGIN
        DELETE FROM Group_Posts
        WHERE Group_Posts.post_id = OLD.id;
    END;
    """
)

# TRIGGER 26: Before deleting a User_Profile, any tuples in the Posts table which contain this User_Profile's id must also be deleted.
cursor.executescript(
    """
    CREATE TRIGGER post_delete_before_user_delete BEFORE DELETE ON User_Profiles
    BEGIN
        DELETE FROM Posts
        WHERE Posts.user_profile_id = OLD.id;
    END;
    """
)

# TRIGGER 27: Before deleting a User_Profile, any tuples in the Comments table which contain this User_Profile's id must also be deleted.
cursor.executescript(
    """
    CREATE TRIGGER comment_delete_before_user_delete BEFORE DELETE ON User_Profiles
    BEGIN
        DELETE FROM Comments
        WHERE Comments.user_profile_id = OLD.id;
    END;
    """
)

# TRIGGER 28: Before deleting a Group, any tuples in the Group_Posts table which contain this Groups's id must also be deleted.
cursor.executescript(
    """
    CREATE TRIGGER group_posts_delete_before_group_delete BEFORE DELETE ON Groups
    BEGIN
        DELETE FROM Group_Posts
        WHERE Group_Posts.group_id = OLD.id;
    END;
    """
)

# TRIGGER 29: Before deleting a Group, any tuples in the Group_Members table which contain this Groups's id must also be deleted.
cursor.executescript(
    """
    CREATE TRIGGER group_members_delete_before_group_delete BEFORE DELETE ON Groups
    BEGIN
        DELETE FROM Group_Members
        WHERE Group_Members.group_id = OLD.id;
    END;
    """
)

# TRIGGER 30: Before deleting a Group, any tuples in the Banned_Profiles table which contain this Groups's id must also be deleted.
cursor.executescript(
    """
    CREATE TRIGGER banned_profiles_delete_before_group_delete BEFORE DELETE ON Groups
    BEGIN
        DELETE FROM Banned_Profiles
        WHERE Banned_Profiles.group_id = OLD.id;
    END;
    """
)

# TRIGGER 31: Before deleting a Post, any tuples in the Post_Reactions table which contain this Post's id must also be deleted.
cursor.executescript(
    """
    CREATE TRIGGER post_reactions_delete_before_post_delete BEFORE DELETE ON Posts
    BEGIN
        DELETE FROM Post_Reactions
        WHERE Post_Reactions.post_id = OLD.id;
    END;
    """
)

# TRIGGER 32: Before deleting a Comment, any tuples in the Comment_Reactions table which contain this Comment's id must also be deleted.
cursor.executescript(
    """
    CREATE TRIGGER comment_reactions_delete_before_comment_delete BEFORE DELETE ON Comments
    BEGIN
        DELETE FROM Comment_Reactions
        WHERE Comment_Reactions.comment_id = OLD.id;
    END;
    """
)

# TRIGGER 33: Before deleting a Post, any tuples in the Comments table which contain this Post's id must also be deleted.
cursor.executescript(
    """
    CREATE TRIGGER comment_delete_before_post_delete BEFORE DELETE ON Posts
    BEGIN
        DELETE FROM Comments
        WHERE Comments.post_id = OLD.id;
    END;
    """
)

# TRIGGER 34: Before deleting a User_Profile, any tuples in the Group_Members table which contain this User_Profiles's id must also be deleted.
cursor.executescript(
    """
    CREATE TRIGGER group_member_delete_before_user_profile_delete BEFORE DELETE ON User_Profiles
    BEGIN
        DELETE FROM Group_Members
        WHERE Group_Members.user_profile_id = OLD.id;
    END;
    """
)






#############
#INDICES
############
# Indices are largely clustered so that they can be isolated by their typical filters
#For example comments are more likely to be shown with the post thus the index is on comment_id
#while posts are shown on a specific user's profile and thus the index is on user_profile_id

cursor.executescript(
    """
    CREATE UNIQUE INDEX idx_username ON User_Profiles(username);
    CREATE UNIQUE INDEX idx_email ON User_Profiles(email);
    CREATE INDEX idx_banned ON Banned_Profiles(group_id);
    CREATE INDEX idx_poster ON Posts(user_profile_id);
    CREATE INDEX idx_group_post ON Group_Posts(group_id);
    CREATE INDEX idx_post_react ON Post_Reactions(post_id);
    CREATE INDEX idx_comm_react ON Comment_Reactions(comment_id);
    CREATE INDEX idx_comments ON Comments(post_id);
    CREATE INDEX idx_blocked ON Blocked(user_profile_id);
    CREATE INDEX idx_group_members ON Group_Members(group_id);
    CREATE INDEX idx_follower ON Followers(user_profile_id);
    CREATE INDEX idx_following ON Following(user_profile_id);
    """
)

####################
# INSERTING SYNTHETIC DATA
####################

# The .csv files in the synthetic_data folder are read using the pandas library and subsequently inserted into the relevant SQL tables.

User_Profile_data = pandas.read_csv("synthetic_data/User_Profile_data.csv")
User_Profile_data.to_sql('User_Profiles', conn, if_exists='append', index=False)

Group_data = pandas.read_csv("synthetic_data/Group_data.csv")
Group_data.to_sql('Groups', conn, if_exists='append', index=False)

# Inserting data into the Groups table will also insert the creators of those groups into the Group_Members table.

Post_data = pandas.read_csv("synthetic_data/Post_data.csv")
Post_data.to_sql('Posts', conn, if_exists='append', index=False)

Comment_data = pandas.read_csv("synthetic_data/Comment_data.csv")
Comment_data.to_sql('Comments', conn, if_exists='append', index=False)

Post_Reaction_data = pandas.read_csv("synthetic_data/Post_Reaction_data.csv")
Post_Reaction_data.to_sql('Post_Reactions', conn, if_exists='append', index=False)

Comment_Reaction_data = pandas.read_csv("synthetic_data/Comment_Reaction_data.csv")
Comment_Reaction_data.to_sql('Comment_Reactions', conn, if_exists='append', index=False)

Group_Post_data = pandas.read_csv("synthetic_data/Group_Post_data.csv")
Group_Post_data.to_sql('Group_Posts', conn, if_exists='append', index=False)

Group_Member_data = pandas.read_csv("synthetic_data/Group_Member_data.csv")
Group_Member_data.to_sql('Group_Members', conn, if_exists='append', index=False)

Follower_data = pandas.read_csv("synthetic_data/Follower_data.csv")
Follower_data.to_sql('Followers', conn, if_exists='append', index=False)

# The Following table has data automatically inserted into it because of TRIGGER 8.
# The reciprocal relationship which was created in the Follower table will be inserted to the Following table.

Blocked_data = pandas.read_csv("synthetic_data/Blocked_data.csv")
Blocked_data.to_sql('Blocked', conn, if_exists='append', index=False)

Banned_Profile_data = pandas.read_csv("synthetic_data/Banned_Profile_data.csv")
Banned_Profile_data.to_sql('Banned_Profiles', conn, if_exists='append', index=False)




####################
# QUERIES
####################

# QUERY 1: How many followers does a random specific User_Profile have? For example, the User_Profile with the username 'rgadesby0'?
print("\n\nQUERY 1")
text = """
        SELECT User_Profiles.username AS USER_NAME, Followers_count.number AS number_of_followers
        FROM Followers_count
        JOIN User_Profiles
            ON User_Profiles.id = Followers_count.user_profile_id
        WHERE Followers_count.user_profile_id = (
            SELECT User_Profiles.id 
            FROM User_Profiles 
            WHERE User_Profiles.username = 'rgadesby0'
			);
        """
df = pandas.read_sql_query(text, conn)
print(df)


# QUERY 2: Which User_Profile has the highest number of followers?
print("\n\nQUERY 2")
text = """
        SELECT Followers_count.user_profile_id AS USER_ID, User_Profiles.username AS USER_NAME, Followers_count.number AS highest_number_of_followers
        FROM Followers_count
        JOIN User_Profiles
            ON User_Profiles.id = Followers_count.user_profile_id
        ORDER BY highest_number_of_followers
        DESC
        LIMIT 1;
        """
df = pandas.read_sql_query(text, conn)
print(df)


# QUERY 3: Which Group has the highest number of members?
print("\n\nQUERY 3")
text = """
        SELECT Groups.id AS GROUP_ID, Groups.groupname AS GROUP_NAME, COUNT(*) AS NUMBER_OF_MEMBERS
        FROM Group_Members
        JOIN Groups
            ON Groups.id = Group_Members.group_id
        GROUP BY Group_Members.group_id
        ORDER BY number_of_members
        DESC
        LIMIT 1;
        """
df = pandas.read_sql_query(text, conn)
print(df)


# QUERY 4: How many reactions does a random specific Post have?
print("\n\nQUERY 4")
text = """
        SELECT Post_Reactions_Count.post_id AS POST_ID, SUM(Post_Reactions_Count.number) AS NUMBER_OF_REACTIONS
        FROM Post_Reactions_Count
        WHERE Post_Reactions_Count.post_id = 53;
        """
df = pandas.read_sql_query(text, conn)
print(df)


# QUERY 5: What are the details of the Posts which have the top 3 number of reactions?
print("\n\nQUERY 5")
text = """
        SELECT Posts.id AS POST_ID, Posts.user_profile_id AS CREATOR_ID, Posts.content as POST_CONTENT, Posts.created_on AS DATE_CREATED
        FROM Posts
        WHERE Posts.id IN (
            SELECT Post_Reactions.post_id
            FROM Post_Reactions
            GROUP BY Post_Reactions.post_id
            ORDER BY COUNT(Post_Reactions.reaction_type)
            DESC
            LIMIT 3
        );
        
        """
df = pandas.read_sql_query(text, conn)
print(df)

# QUERY 6: List how many Posts have been created by User_Profiles in the top three countries in descending order.
print("\n\nQUERY 6")
text = """
        SELECT COUNT(Posts.id) AS NUMBER_OF_POSTS, User_Profiles.country AS COUNTRY
        FROM Posts
        JOIN User_Profiles
            ON Posts.user_profile_id = User_Profiles.id
        GROUP BY User_Profiles.country
        ORDER BY COUNT(Posts.id)
        DESC
        LIMIT 3;
        """
df = pandas.read_sql_query(text, conn)
print(df)

# QUERY 7: Which Groups is a specific User_Profile banned from?
print("\n\nQUERY 7")
text = """
        SELECT Groups.id AS GROUP_ID, Groups.groupname AS GROUP_NAME, User_Profiles.id AS USER_ID, User_Profiles.username AS USER_NAME
        FROM Groups, User_Profiles
        WHERE Groups.id IN (
            SELECT Banned_Profiles.group_id
            FROM Banned_Profiles
            WHERE Banned_Profiles.user_profile_id = 1
        ) AND User_Profiles.id = 1;
        """
df = pandas.read_sql_query(text, conn)
print(df)

# QUERY 8: What are the details of the members of a specific group, and which members are admins? (For example, the Group with the id of 23?)
print("\n\nQUERY 8")
text = """
        SELECT  User_Profiles.username AS USER_NAME,Group_Members.admin AS ADMIN_STATUS, User_Profiles.id AS USER_ID, User_Profiles.email AS USER_EMAIL, User_Profiles.date_of_birth AS USER_DATE_OF_BIRTH, User_Profiles.country AS USER_COUNTRY
        FROM Group_Members
        JOIN User_Profiles
            ON User_profiles.id = Group_Members.user_profile_id
        WHERE Group_Members.group_id = 23;
        """
df = pandas.read_sql_query(text, conn)
print(df)

# QUERY 9: For the groups where a specific User_Profile is an admin, return how many admins there are for each of those groups.
# For example, where the id of the User_Profile = 1.
print("\n\nQUERY 9")
text = """
        SELECT Groups.groupname AS group_name, COUNT(*) AS NO_OF_ADMINS
        FROM Group_Members
		JOIN Groups ON Group_Members.group_id = Groups.id
        WHERE Group_Members.admin = TRUE AND Group_Members.group_id IN (
            SELECT Group_Members.group_id
            FROM Group_Members
            WHERE Group_Members.admin = TRUE
            AND Group_Members.group_id IN (
                SELECT Group_Members.group_id
                FROM Group_Members
                WHERE Group_Members.user_profile_id = 1
            )
            GROUP BY Group_Members.group_id
        )
        GROUP BY Group_Members.group_id;
        """
df = pandas.read_sql_query(text, conn)
print(df)



####################
# UPDATES
####################

# UPDATE 1: Updating a follow request so that it is accepted. Due to TRIGGER 13, the reciprocal relationship in the Following table will also be updated.
print("\n\nUPDATE 1")

text1 = """
        SELECT Followers.*
        FROM Followers
        WHERE Followers.user_profile_id = 85 AND Followers.follower_id = 80;
        """
df = pandas.read_sql_query(text1, conn)
print("\nFOLLOWERS TABLE")
print(df)

text2 = """
        SELECT Following.*
        FROM Following
        WHERE Following.user_profile_id = 80 AND Following.following_id = 85;
        """
df = pandas.read_sql_query(text2, conn)
print("\nFOLLOWING TABLE")
print(df)

cursor.executescript(
    """
    UPDATE Followers
    SET accepted = TRUE
    WHERE user_profile_id = 85 AND follower_id = 80;
    """
)

df = pandas.read_sql_query(text1, conn)
print("\nUPDATED FOLLOWERS TABLE")
print(df)

df = pandas.read_sql_query(text2, conn)
print("\nUPDATED FOLLOWING TABLE")
print(df)



# UPDATE 2: Updating the phone number of a User_Profile.
print("\n\nUPDATE 2")

text = """
        SELECT User_Profiles.phone
        FROM User_Profiles
        WHERE User_Profiles.id = 1
        """
df = pandas.read_sql_query(text, conn)
print("\nUSER_PROFILES TABLE")
print(df)

cursor.executescript(
    """
    UPDATE User_Profiles
    SET phone = '0755795833'
    WHERE id = 1
    """
)

df = pandas.read_sql_query(text, conn)
print("\nUPDATED USER_PROFILES TABLE")
print(df)


# UPDATE 3: Changing the admin status of a group member from FALSE to TRUE.
print("\n\nUPDATE 3")

text = """
        SELECT Group_Members.*
        FROM Group_Members
        WHERE Group_Members.user_profile_id = 24 AND Group_Members.group_id = 66
        """
df = pandas.read_sql_query(text, conn)
print("\nGROUP_MEMBERS TABLE")
print(df)

cursor.executescript(
    """
    UPDATE Group_Members
    SET admin = TRUE
    WHERE user_profile_id = 24 AND group_id = 66
    """
)

df = pandas.read_sql_query(text, conn)
print("\nUPDATED GROUP_MEMBERS TABLE")
print(df)


# UPDATE 4: Changing the privacy setting of a User_Profile.
print("\n\nUPDATE 4")

text = """
        SELECT User_Profiles.id, User_Profiles.privacy
        FROM User_Profiles
        WHERE User_Profiles.id = 40
        """
df = pandas.read_sql_query(text, conn)
print("\nUSER_PROFILES TABLE")
print(df)

cursor.executescript(
    """
    UPDATE User_Profiles
    SET privacy = TRUE
    WHERE id = 40
    """
)

df = pandas.read_sql_query(text, conn)
print("\nUPDATED USER_PROFILES TABLE")
print(df)


# UPDATE 5: Updating the reaction_type attribute on a reaction to a post in the Post_Reactions table.
print("\n\nUPDATE 5")

text = """
        SELECT Post_Reactions.*
        FROM Post_Reactions
        WHERE Post_Reactions.post_id = 76 AND Post_Reactions.user_profile_id = 91
        """
df = pandas.read_sql_query(text, conn)
print("\nPOST_REACTIONS TABLE")
print(df)

cursor.executescript(
    """
    UPDATE Post_Reactions
    SET reaction_type = 'dislike'
    WHERE post_id = 76 AND user_profile_id = 91
    """
)

df = pandas.read_sql_query(text, conn)
print("\nUPDATED POST_REACTIONS TABLE")
print(df)






####################
# DELETIONS
####################

# DELETION 1: Delete a relationship from the Follower table. Due to TRIGGER 16, the reciprocal relationship will also be deleted from the Following table.
print("\n\nDELETION 1")

text1 = """
        SELECT Followers.*
        FROM Followers
        WHERE Followers.user_profile_id = 11 AND Followers.follower_id = 62;
        """
df = pandas.read_sql_query(text1, conn)
print("\nFOLLOWERS TABLE")
print(df)

text2 = """
        SELECT Following.*
        FROM Following
        WHERE Following.following_id = 11 AND Following.user_profile_id = 62;
        """
df = pandas.read_sql_query(text2, conn)
print("\nFOLLOWING TABLE")
print(df)

cursor.executescript(
    """
    DELETE FROM Followers
    WHERE user_profile_id = 11 AND follower_id = 62;
    """
)

df = pandas.read_sql_query(text1, conn)
print("\nFOLLOWERS TABLE AFTER DELETION")
print(df)

df = pandas.read_sql_query(text2, conn)
print("\nFOLLOWING TABLE AFTER DELETION")
print(df)


# DELETION 2: Delete a tuple from the Banned_Profiles table.
print("\n\nDELETION 2")

text = """
        SELECT Banned_Profiles.*
        FROM Banned_Profiles
        WHERE Banned_Profiles.user_profile_id = 16 AND Banned_Profiles.group_id = 1;
        """
df = pandas.read_sql_query(text, conn)
print("\nBANNED_PROFILES TABLE")
print(df)

cursor.executescript(
    """
    DELETE FROM Banned_Profiles
    WHERE user_profile_id = 16 AND group_id = 1;
    """
)

df = pandas.read_sql_query(text, conn)
print("\nBANNED_PROFILES TABLE AFTER DELETION")
print(df)


# DELETION 3: Delete a tuple from the User_Profiles table. Due to many TRIGGERS, many other tuples in tables where the id of the User_Profile is a foreign key will also be deleted.
# For example, delete User_Profile with id = 45.
# As an example, show tuples in the Post_Reactions table and Group_Members table being deleted where the id of the User_Profile is a foreign key.
print("\n\nDELETION 3")

text1 = """
        SELECT User_Profiles.id, User_Profiles.username
        FROM User_Profiles
        WHERE User_Profiles.id = 45
        """
df = pandas.read_sql_query(text1, conn)
print("\nUSER_PROFILES TABLE")
print(df)

text2 = """
        SELECT Post_Reactions.*
        FROM Post_Reactions
        WHERE Post_Reactions.user_profile_id = 45
        """
df = pandas.read_sql_query(text2, conn)
print("\nPOST_REACTIONS TABLE")
print(df)

text3 = """
        SELECT Group_Members.*
        FROM Group_Members
        WHERE Group_Members.user_profile_id = 45
        """
df = pandas.read_sql_query(text3, conn)
print("\nGROUP_MEMBERS TABLE")
print(df)

cursor.executescript(
    """
    DELETE FROM User_Profiles
    WHERE id = 45
    """
)

df = pandas.read_sql_query(text1, conn)
print("\nUSER_PROFILES TABLE AFTER DELETION")
print(df)

df = pandas.read_sql_query(text2, conn)
print("\nPOST_REACTIONS TABLE AFTER DELETION")
print(df)

df = pandas.read_sql_query(text2, conn)
print("\nGROUP_MEMBERS TABLE AFTER DELETION")
print(df)



# DELETION 4: Delete a Group_Member which is the only admin of a Group. The Group_Member will be deleted, along with the corresponding Group because a Group cannot exist without an admin.
# Delete from the Group_Members table where group_id = 1 and user_profile_id = 71. This Group_Member is the only admin for the Group with id = 1.
print("\n\nDELETION 4")

text1 = """
        SELECT *
        FROM Group_Members
        WHERE Group_Members.group_id = 1 AND Group_Members.admin = TRUE
        """
print("\nGROUP_MEMBERS TABLE")
df = pandas.read_sql_query(text1, conn)
print(df)

text2 = """
        SELECT *
        FROM Groups
        WHERE id = 1
        """
print("\nGROUPS TABLE")
df = pandas.read_sql_query(text2, conn)
print(df)

cursor.executescript(
    """
    DELETE FROM Group_Members
    WHERE Group_Members.group_id = 1 AND Group_Members.user_profile_id = 71
    """
)

print("\nGROUP_MEMBERS TABLE AFTER DELETION")
df = pandas.read_sql_query(text1, conn)
print(df)

print("\nGROUPS TABLE AFTER DELETION")
df = pandas.read_sql_query(text2, conn)
print(df)



####################
# INSERTIONS
####################

# INSERTION 1: Inserting into the Blocked table where user_profile_id = 85, and the blocked_id = 80.
# Any Relationship between these two ids in the Following and Followers table must be removed.
print("\n\nINSERTION 1")
text1 = """
        SELECT *
        FROM Followers
        WHERE user_profile_id = 85
        """
df = pandas.read_sql_query(text1, conn)
print("\nFOLLOWERS TABLE")
print(df)

text2 = """
        SELECT *
        FROM Following
        WHERE following_id = 85
        """
df = pandas.read_sql_query(text2, conn)
print("\nFOLLOWING TABLE")
print(df)


cursor.executescript(
    """
    INSERT INTO Blocked
    VALUES (85, 80)
    """
)

df = pandas.read_sql_query(text1, conn)
print("\nFOLLOWERS TABLE AFTER INSERTION INTO BLOCKED")
print(df)

df = pandas.read_sql_query(text2, conn)
print("\nFOLLOWING TABLE AFTER INSERTION INTO BLOCKED")
print(df)




# INSERTION 2: After inserting a User_Profile into the Banned_Profiles table, they should be removed as a member if they are one.
print("\n\nINSERTION 2")
text1 = """
        SELECT *
        FROM Group_Members
        WHERE group_id = 2 AND user_profile_id = 78
        """
df = pandas.read_sql_query(text1, conn)
print("\nGROUP_MEMBERS TABLE")
print(df)

text2 = """
        SELECT *
        FROM Banned_Profiles
        WHERE group_id = 2 AND user_profile_id = 78
        """
df = pandas.read_sql_query(text2, conn)
print("\nBANNED_PROFILES TABLE")
print(df)

cursor.executescript(
    """
    INSERT INTO Banned_Profiles
    VALUES (78, 2)
    """
)

df = pandas.read_sql_query(text1, conn)
print("\nGROUP_MEMBERS TABLE AFTER INSERTION INTO BANNED PROFILES")
print(df)

df = pandas.read_sql_query(text2, conn)
print("\nBANNED_PROFILES TABLE AFTER INSERTION INTO BANNED PROFILES")
print(df)



# INSERTION 3: Isnerting a tuple into the Following table, with the accepted attribute set to FALSE.
# However, because the privacy attribute of the User_Profile relating to the following_id foreign key is set to FALSE, the accepted attribute will be set to TRUE because of TRIGGER 14.
# This relationship will then be updated in the Followers table
print("\n\nINSERTION 3")
text = """
        SELECT id, username, privacy
        FROM User_Profiles
        WHERE id = 16;
        """
df = pandas.read_sql_query(text, conn)
print("\nUSER_PROFILES TABLE")
print(df)

print("\nINSERTING INTO FOLLOWING TABLE WITH ACCEPTED ATTRIBUTE SET TO FALSE")
cursor.executescript(
    """
    INSERT INTO Following
    VALUES (1, 16, FALSE)
    """
)

text = """
        SELECT *
        FROM Following
        WHERE user_profile_id = 1 AND following_id = 16;
        """
df = pandas.read_sql_query(text, conn)
print("\nFOLLOWING TABLE AFTER INSERTION INTO FOLLOWING TABLE")
print(df)

text = """
        SELECT *
        FROM Followers
        WHERE user_profile_id = 16 AND follower_id = 1;
        """
df = pandas.read_sql_query(text, conn)
print("\nFOLLOWERS TABLE AFTER INSERTION INTO FOLLOWING TABLE")
print(df)
