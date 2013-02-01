using System;
using System.Data.SqlClient;
using System.Diagnostics;

namespace NHibernate.AdoNet
{
	/// <summary>
	/// Expose the batch functionality in ADO.Net 2.0
	/// Microsoft in its wisdom decided to make my life hard and mark it internal.
	/// Through the use of Reflection and some delegates magic, I opened up the functionality.
	/// 
	/// Observable performance benefits are 50%+ when used, so it is really worth it.
	/// </summary>
	internal sealed class SqlClientSqlCommandSet : IDisposable
	{
		private static readonly Type sqlCmdSetType;

		private readonly object instance;
		private readonly PropSetter<SqlConnection> connectionSetter;
		private readonly PropSetter<SqlTransaction> transactionSetter;
		private readonly PropSetter<int> commandTimeoutSetter;
		private readonly PropGetter<SqlConnection> connectionGetter;
		private readonly PropGetter<SqlCommand> commandGetter;
		private readonly AppendCommand doAppend;
		private readonly ExecuteNonQueryCommand doExecuteNonQuery;
		private readonly DisposeCommand doDispose;

		private int countOfCommands;

		static SqlClientSqlCommandSet()
		{
			sqlCmdSetType = typeof(SqlCommand).Assembly.GetType("System.Data.SqlClient.SqlCommandSet");
			Debug.Assert(sqlCmdSetType != null, "Could not find SqlCommandSet!");
		}

		public SqlClientSqlCommandSet()
		{
			instance = Activator.CreateInstance(sqlCmdSetType, true);
			connectionSetter = (PropSetter<SqlConnection>)Delegate.CreateDelegate(typeof(PropSetter<SqlConnection>), instance, "set_Connection");
			transactionSetter = (PropSetter<SqlTransaction>)Delegate.CreateDelegate(typeof(PropSetter<SqlTransaction>), instance, "set_Transaction");
			commandTimeoutSetter = (PropSetter<int>)Delegate.CreateDelegate(typeof(PropSetter<int>), instance, "set_CommandTimeout");
			connectionGetter = (PropGetter<SqlConnection>)Delegate.CreateDelegate(typeof(PropGetter<SqlConnection>), instance, "get_Connection");
			commandGetter = (SqlClientSqlCommandSet.PropGetter<SqlCommand>)Delegate.CreateDelegate(typeof(SqlClientSqlCommandSet.PropGetter<SqlCommand>), instance, "get_BatchCommand");
			doAppend = (AppendCommand)Delegate.CreateDelegate(typeof(AppendCommand), instance, "Append");
			doExecuteNonQuery = (ExecuteNonQueryCommand)Delegate.CreateDelegate(typeof(ExecuteNonQueryCommand), instance, "ExecuteNonQuery");
			doDispose = (DisposeCommand)Delegate.CreateDelegate(typeof(DisposeCommand), instance, "Dispose");
		}

		/// <summary>
		/// Append a command to the batch
		/// </summary>
		/// <param name="command"></param>
		public void Append(SqlCommand command)
		{
			AssertHasParameters(command);
            foreach (SqlParameter p in command.Parameters)
                if (!p.ParameterName.StartsWith("@"))
                    p.ParameterName = "@" + p.ParameterName;
			doAppend(command);
			countOfCommands++;
		}

		/// <summary>
		/// This is required because SqlClient.SqlCommandSet will throw if 
		/// the command has no parameters.
		/// </summary>
		/// <param name="command"></param>
		private static void AssertHasParameters(SqlCommand command)
		{
			if (command.Parameters.Count == 0)
			{
				throw new ArgumentException(
					"A command in SqlCommandSet must have parameters. You can't pass hardcoded sql strings.");
			}
		}

		/// <summary>
		/// Return the batch command to be executed
		/// </summary>
		public SqlCommand BatchCommand
		{
			get { return commandGetter(); }
		}

		/// <summary>
		/// The number of commands batched in this instance
		/// </summary>
		public int CountOfCommands
		{
			get { return countOfCommands; }
		}

		/// <summary>
		/// Executes the batch
		/// </summary>
		/// <returns>
		/// This seems to be returning the total number of affected rows in all queries
		/// </returns>
		public int ExecuteNonQuery()
		{
			if (Connection == null)
			{
				throw new InvalidOperationException(
					"Connection was not set! You must set the connection property before calling ExecuteNonQuery()");
			}

			if (CountOfCommands == 0)
				return 0;

			return doExecuteNonQuery();
		}

		public SqlConnection Connection
		{
			get { return connectionGetter(); }
			set { connectionSetter(value); }
		}

		public SqlTransaction Transaction
		{
			set { transactionSetter(value); }
		}

		public int CommandTimeout
		{
			set { commandTimeoutSetter(value); }
		}

		///<summary>
		/// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
		///</summary>
		public void Dispose()
		{
			doDispose();
		}

		#region Delegate Definations

		private delegate void PropSetter<in T>(T item);

		private delegate T PropGetter<out T>();

		private delegate void AppendCommand(SqlCommand command);

		private delegate int ExecuteNonQueryCommand();

		private delegate void DisposeCommand();

		#endregion
	}
}
