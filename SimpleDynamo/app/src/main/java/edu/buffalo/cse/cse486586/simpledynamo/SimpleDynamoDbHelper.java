package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

public class SimpleDynamoDbHelper extends SQLiteOpenHelper {

    /*Below modified code is based on understanding
    https://developer.android.com/training/data-storage/sqlite*/

    //Database name
    private static final String DATABASE_NAME = "simpleDynamo.db";
    //To track the database schema changes
    private static final int VERSION = 1;
    //Table Name
    public static final String TABLE_NAME = "simpleDynamo";
    private static final String TAG = SimpleDynamoDbHelper.class.getName();

    public static final String KEY_FIELD = "key";
    public static final String VALUE_FIELD = "value";
    public static final String PORT_FIELD = "port";

    SimpleDynamoDbHelper(Context context){super(context, DATABASE_NAME, null, VERSION);
        Log.d(TAG, "Table "+ SimpleDynamoDbHelper.TABLE_NAME+ "path "+
                context.getDatabasePath(DATABASE_NAME).getAbsolutePath());
    }

    @Override
    public void onCreate(SQLiteDatabase sqLiteDatabase) {

        final String CREATE_TABLE =
                "CREATE TABLE " + SimpleDynamoDbHelper.TABLE_NAME + " (" +
                KEY_FIELD + " TEXT NOT NULL PRIMARY KEY, " +
                VALUE_FIELD + " TEXT NOT NULL, " + PORT_FIELD + " TEXT NOT NULL);";

        sqLiteDatabase.execSQL(CREATE_TABLE);
        Log.i(TAG, "Table "+ SimpleDynamoDbHelper.TABLE_NAME+ " created!");

    }

    @Override
    public void onUpgrade(SQLiteDatabase sqLiteDatabase, int oldVersion, int newVersion) {
        sqLiteDatabase.execSQL("DROP TABLE IF EXISTS "+ SimpleDynamoDbHelper.TABLE_NAME);
        onCreate(sqLiteDatabase);

        Log.i(TAG, "Table "+ SimpleDynamoDbHelper.TABLE_NAME+ " deleted!");
    }
}
